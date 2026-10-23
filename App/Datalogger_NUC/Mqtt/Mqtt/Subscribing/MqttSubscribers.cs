using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Publishing;

namespace Mqtt.Mqtt.Subscribing;

public sealed class MqttSubscriber
{
    private readonly ILogger<MqttSubscriber> _logger;
    private readonly MqttService _mqtt;
    private readonly MqttTopicBuilder _topicBuilder;
    private readonly MqttConnectionManager _connection;
    private readonly MqttPublishManager _publishManager;

    private readonly Dictionary<string, Func<string, string, Task>> _handlers;

    public MqttSubscriber(
        ILogger<MqttSubscriber> logger,
        MqttService mqtt,
        MqttTopicBuilder topicBuilder,
        MqttConnectionManager connection,
        MqttPublishManager publishManager
    )
    {
        _logger = logger;
        _mqtt = mqtt;
        _topicBuilder = topicBuilder;
        _connection = connection;
        _publishManager = publishManager;

        _handlers = new(StringComparer.OrdinalIgnoreCase)
        {
            [MqttTopics.Opera] = HandleOperaAsync,
            [MqttTopics.sSet1] = HandleSSet1Async,
            [MqttTopics.MePDV] = HandleMePDVAsync,
            [MqttTopics.sCoFi] = HandleSCoFiAsync,

            // Thêm topic subscribe mới tại đây.
        };
    }

    // =========================================================
    // RUN
    // =========================================================

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _mqtt.MessageReceived += OnMessageReceivedAsync;

        _logger.LogInformation("MQTT subscriber started.");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // -------------------------------------------------
                // 1. CHỜ MQTT CONNECTED
                // -------------------------------------------------

                await _connection.WaitUntilConnectedAsync(cancellationToken);

                if (!_mqtt.IsConnected)
                {
                    continue;
                }

                // -------------------------------------------------
                // 2. CONNECTION MỚI -> SUBSCRIBE
                // -------------------------------------------------

                if (!_connection.IsReady)
                {
                    try
                    {
                        await SubscribeAllAsync(cancellationToken);

                        _connection.MarkReady();

                        _publishManager.Request(MqttTopics.MePDV);
                    }
                    catch (OperationCanceledException)
                        when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "MQTT subscribe failed | Server={Server}",
                            _connection.CurrentServerName
                        );
                        _connection.RequestReconnect();
                    }

                    continue;
                }

                // -------------------------------------------------
                // 3. ĐÃ READY
                // -------------------------------------------------

                await WaitUntilConnectionLostAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown bình thường.
        }
        finally
        {
            _mqtt.MessageReceived -= OnMessageReceivedAsync;

            _connection.MarkNotReady();

            _logger.LogInformation("MQTT subscriber stopped.");
        }
    }

    // =========================================================
    // WAIT CONNECTION LOST
    // =========================================================

    private async Task WaitUntilConnectionLostAsync(CancellationToken cancellationToken)
    {
        while (_mqtt.IsConnected && _connection.IsReady)
        {
            await Task.Delay(100, cancellationToken);
        }
    }

    // =========================================================
    // SUBSCRIBE
    // =========================================================

    private async Task SubscribeAllAsync(CancellationToken cancellationToken)
    {
        string topic = _topicBuilder.BuildSubscribeTopic();

        await _mqtt.SubscribeAsync(topic, cancellationToken);

        _logger.LogInformation(
            "MQTT subscribed | Topic={Topic} | Server={Server}",
            topic,
            _connection.CurrentServerName
        );
    }

    // =========================================================
    // RECEIVE MESSAGE
    // =========================================================

    private async Task OnMessageReceivedAsync(string topic, string payload)
    {
        try
        {
            string logicalTopic = GetLogicalTopic(topic);

            if (!_handlers.TryGetValue(logicalTopic, out Func<string, string, Task>? handler))
            {
                _logger.LogDebug("MQTT RX no handler | Topic={Topic}", topic);

                return;
            }

            await handler(topic, payload);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MQTT RX processing failed | Topic={Topic}", topic);
        }
    }

    // =========================================================
    // GET LOGICAL TOPIC
    // =========================================================

    private static string GetLogicalTopic(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return string.Empty;
        }

        int index = topic.LastIndexOf('/');

        if (index < 0)
        {
            return topic;
        }

        if (index == topic.Length - 1)
        {
            return string.Empty;
        }

        return topic[(index + 1)..];
    }

    // =========================================================
    // OPERA CALLBACK
    // =========================================================

    private Task HandleOperaAsync(string topic, string payload)
    {
        _logger.LogInformation("MQTT RX Opera | Topic={Topic} | Payload={Payload}", topic, payload);

        // Xử lý Opera tại đây.

        return Task.CompletedTask;
    }

    // =========================================================
    // SSET1 CALLBACK
    // =========================================================

    private Task HandleSSet1Async(string topic, string payload)
    {
        _logger.LogInformation("MQTT RX sSet1 | Topic={Topic} | Payload={Payload}", topic, payload);

        // Xử lý sSet1 tại đây.

        return Task.CompletedTask;
    }

    // =========================================================
    // MEPDV CALLBACK
    // =========================================================

    private Task HandleMePDVAsync(string topic, string payload)
    {
        _logger.LogInformation("MQTT RX MePDV | Topic={Topic} | Payload={Payload}", topic, payload);

        // Xử lý MePDV tại đây.

        return Task.CompletedTask;
    }

    // =========================================================
    // SCOFI CALLBACK
    // =========================================================

    private Task HandleSCoFiAsync(string topic, string payload)
    {
        _logger.LogInformation("MQTT RX sCoFi | Topic={Topic} | Payload={Payload}", topic, payload);

        // Xử lý sCoFi tại đây.

        return Task.CompletedTask;
    }
}
