using Microsoft.Extensions.Options;
using Mqtt.Configuration;

namespace Mqtt.Mqtt.Core;

public sealed class MqttConnectionManager
{
    private const int MaxConnectFailuresPerServer = 5;
    private const int ConnectionLoopDelayMilliseconds = 1000;

    private readonly ILogger<MqttConnectionManager> _logger;
    private readonly MqttService _mqtt;
    private readonly AppSettings _settings;

    // =========================================================
    // SYNCHRONIZATION
    // =========================================================

    // Đảm bảo Connect / Disconnect không chạy đồng thời.
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // Đánh thức connection loop ngay lập tức khi cần reconnect.
    private readonly SemaphoreSlim _reconnectSignal = new(0, 1);

    // Bảo vệ state nội bộ.
    private readonly object _stateLock = new();

    // =========================================================
    // CONNECTION STATE
    // =========================================================

    private MqttServerType _currentServer = MqttServerType.Main;

    private int _consecutiveConnectFailures;

    private bool _reconnectRequested;
    private bool _switchServerOnReconnect;

    // MQTT CONNECT
    private TaskCompletionSource<bool> _connectedTcs = CreateSignal();

    // MQTT CONNECT + SUBSCRIBER
    private TaskCompletionSource<bool> _readyTcs = CreateSignal();

    // =========================================================
    // PROPERTIES
    // =========================================================

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

    // =========================================================
    // MQTT SERVER IP ĐANG KẾT NỐI
    // =========================================================

    public string CurrentServerIp
    {
        get
        {
            MqttServerType server = CurrentServer;

            MqttServerSettings settings = GetServerSettings(server);

            return settings.IP;
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

    // =========================================================
    // CONSTRUCTOR
    // =========================================================

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

                // -------------------------------------------------
                // RECONNECT REQUEST
                // -------------------------------------------------

                ReconnectRequest request = ConsumeReconnectRequest();

                if (request.Requested)
                {
                    if (request.SwitchServer)
                    {
                        SwitchServer();
                    }

                    await DisconnectAsync(cancellationToken);
                }

                // -------------------------------------------------
                // CONNECT
                // -------------------------------------------------

                if (!_mqtt.IsConnected)
                {
                    MarkDisconnected();

                    ConnectResult result = await TryConnectCurrentServerAsync(cancellationToken);

                    if (result == ConnectResult.SwitchServer)
                    {
                        SwitchServer();
                    }
                }

                // -------------------------------------------------
                // WAIT
                // -------------------------------------------------

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
                _logger.LogInformation(
                    "MQTT connecting | Server={Server} | IP={IP} | Attempt={Attempt}/{Max}",
                    serverType,
                    server.IP,
                    _consecutiveConnectFailures + 1,
                    MaxConnectFailuresPerServer
                );

                await _mqtt.ConnectAsync(server, cancellationToken);

                // Connect thành công.
                _consecutiveConnectFailures = 0;

                MarkConnected();
                MarkNotReady();

                _logger.LogInformation(
                    "MQTT connected | Server={Server} | IP={IP}",
                    serverType,
                    server.IP
                );

                return ConnectResult.Connected;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _consecutiveConnectFailures++;

                _logger.LogWarning(
                    ex,
                    "MQTT connection failed | Server={Server} | IP={IP} | Failure={Failure}/{Max}",
                    serverType,
                    server.IP,
                    _consecutiveConnectFailures,
                    MaxConnectFailuresPerServer
                );

                if (_consecutiveConnectFailures >= MaxConnectFailuresPerServer)
                {
                    _consecutiveConnectFailures = 0;

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
                await _mqtt.DisconnectAsync(cancellationToken);
            }

            _consecutiveConnectFailures = 0;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    // =========================================================
    // MQTT DISCONNECTED EVENT
    // =========================================================

    private void OnMqttDisconnected()
    {
        MarkDisconnected();
        MarkNotReady();

        _logger.LogWarning(
            "MQTT disconnected | Server={Server} | IP={IP}",
            CurrentServerName,
            CurrentServerIp
        );

        // Đánh thức connection loop ngay lập tức.
        SignalReconnect();
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
        /*
         * Chờ tối đa 1000 ms.
         *
         * Nếu có reconnect signal:
         *     -> tỉnh dậy ngay.
         *
         * Nếu không:
         *     -> sau 1000 ms kiểm tra connection lại.
         */
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
