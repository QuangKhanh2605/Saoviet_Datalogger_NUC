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
    private readonly AppSettings _settings;

    public MqttWorker(
        ILogger<MqttWorker> logger,
        MqttConnectionManager connection,
        MqttSubscriber subscriber,
        MqttPublishManager publishManager,
        IOptions<AppSettings> options
    )
    {
        _logger = logger;
        _connection = connection;
        _subscriber = subscriber;
        _publishManager = publishManager;
        _settings = options.Value;
    }

    // =========================================================
    // REQUEST PUBLISH
    // =========================================================

    public void RequestPublish(string topic, string? payload = null)
    {
        _publishManager.Request(topic, payload);
    }

    // =========================================================
    // WORKER
    // =========================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_settings.Mqtt.Enabled)
        {
            _logger.LogInformation("MQTT disabled.");

            return;
        }

        _logger.LogInformation("MQTT worker started.");

        Task connectionTask = _connection.RunAsync(stoppingToken);

        Task subscribeTask = _subscriber.RunAsync(stoppingToken);

        Task publishTask = _publishManager.RunAsync(stoppingToken);

        try
        {
            await Task.WhenAll(connectionTask, subscribeTask, publishTask);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MQTT worker failed.");
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
}
