using Microsoft.Extensions.Options;
using Mqtt.Configuration;
using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Publishing;
using Mqtt.Mqtt.Subscribing;

namespace Mqtt.Mqtt;

public sealed class MqttWorker : BackgroundService
{
    private readonly ILogger<MqttWorker> _logger;
    private readonly MqttConnectionManager _connection;
    private readonly MqttSubscriber _subscriber;
    private readonly MqttPublishManager _publishManager;
    private readonly IOptionsMonitor<AppSettings> _settings;

    private readonly SemaphoreSlim _restartSignal = new(0, 1);

    private static readonly TimeSpan SessionRestartDelay = TimeSpan.FromSeconds(1);

    public MqttWorker(
        ILogger<MqttWorker> logger,
        MqttConnectionManager connection,
        MqttSubscriber subscriber,
        MqttPublishManager publishManager,
        IOptionsMonitor<AppSettings> options
    )
    {
        _logger = logger;
        _connection = connection;
        _subscriber = subscriber;
        _publishManager = publishManager;
        _settings = options;
    }

    // =========================================================
    // REQUEST PUBLISH
    // =========================================================

    public void RequestPublish(string topic, string? payload = null)
    {
        _publishManager.Request(topic, payload);
    }

    // =========================================================
    // REQUEST RESTART
    // =========================================================

    public void RequestRestart()
    {
        try
        {
            _restartSignal.Release();

            _logger.LogInformation("MQTT restart requested.");
        }
        catch (SemaphoreFullException)
        {
            // Đã có yêu cầu restart đang chờ xử lý.
        }
    }

    // =========================================================
    // WAIT FOR RESTART
    // =========================================================

    private async Task WaitForRestartAsync(CancellationToken cancellationToken)
    {
        await _restartSignal.WaitAsync(cancellationToken);

        // Gộp các yêu cầu restart đang chờ thành một lần.
        while (_restartSignal.Wait(0)) { }
    }

    // =========================================================
    // MAIN WORKER
    // =========================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("MQTT worker started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // MQTT bị tắt: chờ yêu cầu restart hoặc shutdown.
                    if (!_settings.CurrentValue.Mqtt.Enabled)
                    {
                        _logger.LogInformation("MQTT disabled. Waiting for restart request.");

                        await WaitForRestartAsync(stoppingToken);

                        continue;
                    }

                    _logger.LogInformation("Starting MQTT session.");

                    await RunSessionUntilRestartAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Unexpected MQTT worker error. " + "The worker will retry."
                    );
                }

                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                // Ngắt kết nối sau khi session kết thúc hoặc gặp lỗi.
                try
                {
                    await _connection.DisconnectAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "MQTT disconnect failed.");
                }

                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                _logger.LogInformation(
                    "MQTT worker will retry in {DelaySeconds} second(s).",
                    SessionRestartDelay.TotalSeconds
                );

                await Task.Delay(SessionRestartDelay, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Worker được yêu cầu dừng.
        }
        finally
        {
            try
            {
                await _connection.DisconnectAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "MQTT shutdown disconnect failed.");
            }

            _logger.LogInformation("MQTT worker stopped.");
        }
    }

    // =========================================================
    // RUN SESSION UNTIL RESTART
    // =========================================================

    private async Task RunSessionUntilRestartAsync(CancellationToken stoppingToken)
    {
        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        using var restartCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        Task sessionTask = RunSessionAsync(sessionCts.Token);

        Task restartTask = WaitForRestartAsync(restartCts.Token);

        try
        {
            Task completed = await Task.WhenAny(sessionTask, restartTask);

            if (completed == restartTask)
            {
                // Có yêu cầu restart hoặc worker đang shutdown.
                await restartTask;

                _logger.LogInformation("MQTT restart signal received.");
            }
            else
            {
                // Nếu session lỗi, exception sẽ được chuyển lên
                // ExecuteAsync để ghi log và khởi động lại.
                await sessionTask;

                _logger.LogWarning("MQTT session ended unexpectedly.");
            }
        }
        finally
        {
            // Dừng tất cả task thuộc session cũ.
            await sessionCts.CancelAsync();

            // Không để task chờ restart tồn tại sau session.
            await restartCts.CancelAsync();

            try
            {
                await sessionTask;
            }
            catch (OperationCanceledException) when (sessionCts.IsCancellationRequested)
            {
                // Session bị hủy theo yêu cầu.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MQTT session task failed.");
            }

            try
            {
                await restartTask;
            }
            catch (OperationCanceledException) when (restartCts.IsCancellationRequested)
            {
                // Task chờ restart đã được hủy.
            }
        }
    }

    // =========================================================
    // RUN ONE SESSION
    // =========================================================

    private async Task RunSessionAsync(CancellationToken cancellationToken)
    {
        Task connectionTask = _connection.RunAsync(cancellationToken);

        Task subscribeTask = _subscriber.RunAsync(cancellationToken);

        Task publishTask = _publishManager.RunAsync(cancellationToken);

        await Task.WhenAll(connectionTask, subscribeTask, publishTask);
    }
}
