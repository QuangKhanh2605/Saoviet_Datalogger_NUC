using Microsoft.Extensions.Options;
using Mqtt.Configuration;
using MQTTnet;

namespace Mqtt.Mqtt.Core;

public sealed class MqttConnectionManager
{
    private const int MaxConnectFailuresPerServer = 5;
    private const int ConnectionLoopDelayMilliseconds = 1000;

    private readonly ILogger<MqttConnectionManager> _logger;
    private readonly MqttService _mqtt;
    private readonly AppSettings _settings;

    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _reconnectSignal = new(0, 1);
    private readonly object _stateLock = new();

    private MqttServerType _currentServer = MqttServerType.Main;

    // Số lần kết nối thất bại liên tiếp trước khi MQTT đạt READY.
    private int _consecutiveConnectFailures;

    // Ngăn sự kiện Disconnect chủ động bị tính là lỗi.
    private int _intentionalDisconnect;

    private bool _reconnectRequested;
    private bool _switchServerOnReconnect;

    private TaskCompletionSource<bool> _connectedTcs = CreateSignal();
    private TaskCompletionSource<bool> _readyTcs = CreateSignal();

    public MqttServerType CurrentServer
    {
        get
        {
            lock (_stateLock)
            {
                return _currentServer;
            }
        }
    }

    public string CurrentServerName => CurrentServer == MqttServerType.Main ? "Main" : "Backup";

    public string CurrentServerIp
    {
        get
        {
            MqttServerType server = CurrentServer;
            return GetServerSettings(server).IP;
        }
    }

    public bool IsConnected => _mqtt.IsConnected;

    public bool IsReady
    {
        get
        {
            if (!_mqtt.IsConnected)
            {
                return false;
            }

            lock (_stateLock)
            {
                return _readyTcs.Task.IsCompletedSuccessfully;
            }
        }
    }

    public MqttConnectionManager(
        ILogger<MqttConnectionManager> logger,
        MqttService mqtt,
        IOptions<AppSettings> options
    )
    {
        _logger = logger;
        _mqtt = mqtt;
        _settings = options.Value;

        _mqtt.Disconnected += OnMqttDisconnected;
    }

    // =========================================================
    // RUN
    // =========================================================

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("MQTT connection manager started.");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                cancellationToken.ThrowIfCancellationRequested();

                ReconnectRequest request = ConsumeReconnectRequest();

                if (request.Requested)
                {
                    if (request.SwitchServer)
                    {
                        SwitchServer();
                    }

                    await DisconnectAsync(cancellationToken);
                }

                if (!_mqtt.IsConnected)
                {
                    MarkDisconnected();

                    ConnectResult result = await TryConnectCurrentServerAsync(cancellationToken);

                    if (result == ConnectResult.SwitchServer)
                    {
                        SwitchServer();
                    }
                }

                await WaitForNextConnectionCheckAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown bình thường.
        }
        finally
        {
            await DisconnectAsync(CancellationToken.None);

            _logger.LogInformation("MQTT connection manager stopped.");
        }
    }

    // =========================================================
    // CONNECT
    // =========================================================

    private async Task<ConnectResult> TryConnectCurrentServerAsync(
        CancellationToken cancellationToken
    )
    {
        await _connectionLock.WaitAsync(cancellationToken);

        try
        {
            if (_mqtt.IsConnected)
            {
                MarkConnected();
                return ConnectResult.Connected;
            }

            MqttServerType serverType = CurrentServer;
            MqttServerSettings server = GetServerSettings(serverType);

            try
            {
                int attempt = Volatile.Read(ref _consecutiveConnectFailures) + 1;

                _logger.LogInformation(
                    "MQTT connecting | Server={Server} | IP={IP} | Attempt={Attempt}/{Max}",
                    serverType,
                    server.IP,
                    attempt,
                    MaxConnectFailuresPerServer
                );

                await _mqtt.ConnectAsync(server, cancellationToken);

                MarkConnected();
                MarkNotReady();

                _logger.LogInformation(
                    "MQTT TCP session connected | Server={Server} | IP={IP} | Attempt={Attempt}/{Max}",
                    serverType,
                    server.IP,
                    Volatile.Read(ref _consecutiveConnectFailures) + 1,
                    MaxConnectFailuresPerServer
                );

                // Không đặt lại bộ đếm ở đây.
                // Chỉ MarkReady mới xác nhận kết nối đã ổn định.

                return ConnectResult.Connected;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                int failures = Interlocked.Increment(ref _consecutiveConnectFailures);

                _logger.LogWarning(
                    ex,
                    "MQTT connection failed | Server={Server} | IP={IP} | Failure={Failure}/{Max}",
                    serverType,
                    server.IP,
                    failures,
                    MaxConnectFailuresPerServer
                );

                if (failures >= MaxConnectFailuresPerServer)
                {
                    Interlocked.Exchange(ref _consecutiveConnectFailures, 0);

                    return ConnectResult.SwitchServer;
                }

                return ConnectResult.Failed;
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    // =========================================================
    // CONNECTED SIGNAL
    // =========================================================

    public async Task WaitUntilConnectedAsync(CancellationToken cancellationToken)
    {
        while (!IsConnected)
        {
            Task connectedTask;

            lock (_stateLock)
            {
                connectedTask = _connectedTcs.Task;
            }

            await connectedTask.WaitAsync(cancellationToken);
        }
    }

    private void MarkConnected()
    {
        lock (_stateLock)
        {
            _connectedTcs.TrySetResult(true);
        }
    }

    private void MarkDisconnected()
    {
        lock (_stateLock)
        {
            if (!_connectedTcs.Task.IsCompleted)
            {
                return;
            }

            _connectedTcs = CreateSignal();
        }
    }

    // =========================================================
    // READY SIGNAL
    // =========================================================

    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        while (!IsReady)
        {
            Task readyTask;

            lock (_stateLock)
            {
                readyTask = _readyTcs.Task;
            }

            await readyTask.WaitAsync(cancellationToken);
        }
    }

    public void MarkReady()
    {
        lock (_stateLock)
        {
            if (!_mqtt.IsConnected)
            {
                return;
            }

            _readyTcs.TrySetResult(true);
        }

        // MQTT đã kết nối và hoàn tất khởi tạo Subscriber.
        // Bắt đầu chu kỳ đếm lỗi mới từ trạng thái ổn định.
        Interlocked.Exchange(ref _consecutiveConnectFailures, 0);

        _logger.LogInformation(
            "MQTT READY | Server={Server} | IP={IP}",
            CurrentServerName,
            CurrentServerIp
        );
    }

    public void MarkNotReady()
    {
        lock (_stateLock)
        {
            if (!_readyTcs.Task.IsCompleted)
            {
                return;
            }

            _readyTcs = CreateSignal();
        }
    }

    // =========================================================
    // RECONNECT REQUEST
    // =========================================================

    public void RequestReconnect(bool switchServer = false)
    {
        lock (_stateLock)
        {
            _reconnectRequested = true;

            if (switchServer)
            {
                _switchServerOnReconnect = true;
            }
        }

        _logger.LogWarning(
            "MQTT reconnect requested | Server={Server} | IP={IP} | SwitchServer={SwitchServer}",
            CurrentServerName,
            CurrentServerIp,
            switchServer
        );

        SignalReconnect();
    }

    private void SignalReconnect()
    {
        if (_reconnectSignal.CurrentCount == 0)
        {
            _reconnectSignal.Release();
        }
    }

    private ReconnectRequest ConsumeReconnectRequest()
    {
        lock (_stateLock)
        {
            if (!_reconnectRequested)
            {
                return default;
            }

            ReconnectRequest request = new(true, _switchServerOnReconnect);

            _reconnectRequested = false;
            _switchServerOnReconnect = false;

            return request;
        }
    }

    // =========================================================
    // DISCONNECT
    // =========================================================

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        MarkNotReady();
        MarkDisconnected();

        await _connectionLock.WaitAsync(cancellationToken);

        try
        {
            if (_mqtt.IsConnected)
            {
                Interlocked.Exchange(ref _intentionalDisconnect, 1);

                try
                {
                    await _mqtt.DisconnectAsync(cancellationToken);
                }
                finally
                {
                    Interlocked.Exchange(ref _intentionalDisconnect, 0);
                }
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    // =========================================================
    // MQTT DISCONNECTED EVENT
    // =========================================================

    private void OnMqttDisconnected(MqttClientDisconnectedEventArgs args)
    {
        MarkDisconnected();
        MarkNotReady();

        bool intentional = Volatile.Read(ref _intentionalDisconnect) != 0;

        _logger.LogWarning(
            "MQTT disconnected | Server={Server} | IP={IP} | ClientWasConnected={ClientWasConnected} | Reason={Reason} | ReasonString={ReasonString} | Exception={Exception} | Intentional={Intentional}",
            CurrentServerName,
            CurrentServerIp,
            args.ClientWasConnected,
            args.Reason,
            args.ReasonString,
            args.Exception?.ToString() ?? "None",
            intentional
        );

        if (intentional || !args.ClientWasConnected)
        {
            return;
        }

        // Kết nối đã được thiết lập nhưng bị ngắt bất ngờ.
        // Tính lần ngắt này vào chuỗi lỗi kết nối.
        int failures = Interlocked.Increment(ref _consecutiveConnectFailures);

        _logger.LogWarning(
            "MQTT connection lost | Server={Server} | ConsecutiveFailures={Failures}/{Max}",
            CurrentServerName,
            failures,
            MaxConnectFailuresPerServer
        );

        if (failures >= MaxConnectFailuresPerServer)
        {
            Interlocked.Exchange(ref _consecutiveConnectFailures, 0);

            RequestReconnect(switchServer: true);
        }

        // Không SignalReconnect ở đây khi chưa đủ 5 lần.
        // Vòng lặp sẽ thử lại sau khoảng chờ kiểm tra kết nối.
    }

    // =========================================================
    // SERVER FAILOVER
    // =========================================================

    private void SwitchServer()
    {
        lock (_stateLock)
        {
            _currentServer =
                _currentServer == MqttServerType.Main ? MqttServerType.Backup : MqttServerType.Main;
        }

        Interlocked.Exchange(ref _consecutiveConnectFailures, 0);

        _logger.LogWarning(
            "MQTT switching server | NewServer={Server} | IP={IP}",
            CurrentServerName,
            CurrentServerIp
        );
    }

    // =========================================================
    // SERVER SETTINGS
    // =========================================================

    private MqttServerSettings GetServerSettings(MqttServerType server)
    {
        return server == MqttServerType.Main ? _settings.Mqtt.Main : _settings.Mqtt.Backup;
    }

    // =========================================================
    // CONNECTION LOOP WAIT
    // =========================================================

    private async Task WaitForNextConnectionCheckAsync(CancellationToken cancellationToken)
    {
        await _reconnectSignal.WaitAsync(
            TimeSpan.FromMilliseconds(ConnectionLoopDelayMilliseconds),
            cancellationToken
        );
    }

    // =========================================================
    // SIGNAL FACTORY
    // =========================================================

    private static TaskCompletionSource<bool> CreateSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    // =========================================================
    // INTERNAL TYPES
    // =========================================================

    public enum MqttServerType
    {
        Main,
        Backup,
    }

    private enum ConnectResult
    {
        Connected,
        Failed,
        SwitchServer,
    }

    private readonly record struct ReconnectRequest(bool Requested, bool SwitchServer);
}
