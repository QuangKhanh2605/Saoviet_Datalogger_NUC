using System.Text;
using Mqtt.Configuration;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace Mqtt.Mqtt.Core;

public sealed class MqttService
{
    private readonly ILogger<MqttService> _logger;
    private readonly AppSettings _settings;
    private readonly IMqttClient _client;

    public event Func<string, string, Task>? MessageReceived;

    public event Action? Disconnected;

    public bool IsConnected => _client.IsConnected;

    public MqttService(ILogger<MqttService> logger, IOptions<AppSettings> options)
    {
        _logger = logger;
        _settings = options.Value;

        MqttClientFactory factory = new();

        _client = factory.CreateMqttClient();

        _client.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;

        _client.DisconnectedAsync += OnDisconnectedAsync;
    }

    // =========================================================
    // CONNECT
    // =========================================================

    public async Task ConnectAsync(MqttServerSettings server, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(server);

        if (string.IsNullOrWhiteSpace(server.IP))
        {
            throw new InvalidOperationException("MQTT server IP is empty.");
        }

        if (server.Port <= 0)
        {
            throw new InvalidOperationException($"Invalid MQTT port: {server.Port}");
        }

        MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithClientId(_settings.Device.Id)
            .WithTcpServer(server.IP, server.Port)
            // MQTT 3.1
            .WithProtocolVersion(MqttProtocolVersion.V310)
            .WithCredentials(server.User, server.Pass)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(Math.Max(1, _settings.Mqtt.KeepAliveSeconds)))
            .WithCleanSession()
            .Build();

        await _client.ConnectAsync(options, cancellationToken);
    }

    // =========================================================
    // DISCONNECT
    // =========================================================

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (!_client.IsConnected)
        {
            return;
        }

        try
        {
            await _client.DisconnectAsync(new MqttClientDisconnectOptions(), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MQTT disconnect failed.");
        }
    }

    // =========================================================
    // PUBLISH
    // =========================================================

    public async Task PublishAsync(
        string topic,
        string payload,
        CancellationToken cancellationToken
    )
    {
        if (!_client.IsConnected)
        {
            throw new InvalidOperationException("MQTT client is not connected.");
        }

        MqttQualityOfServiceLevel qos = ToQos(_settings.Mqtt.PublishQos);

        MqttApplicationMessage message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(Encoding.UTF8.GetBytes(payload))
            .WithQualityOfServiceLevel(qos)
            .Build();

        await _client.PublishAsync(message, cancellationToken);
    }

    // =========================================================
    // SUBSCRIBE
    // =========================================================

    public async Task SubscribeAsync(string topic, CancellationToken cancellationToken)
    {
        if (!_client.IsConnected)
        {
            throw new InvalidOperationException("MQTT client is not connected.");
        }

        MqttQualityOfServiceLevel qos = ToQos(_settings.Mqtt.SubscribeQos);

        MqttClientSubscribeOptions options = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(topic, qos)
            .Build();

        await _client.SubscribeAsync(options, cancellationToken);
    }

    // =========================================================
    // RECEIVE
    // =========================================================

    private async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        try
        {
            string topic = args.ApplicationMessage.Topic;

            string payload = args.ApplicationMessage.ConvertPayloadToString();

            Func<string, string, Task>? handler = MessageReceived;

            if (handler != null)
            {
                await handler(topic, payload);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled MQTT receive error.");
        }
    }

    // =========================================================
    // DISCONNECTED EVENT
    // =========================================================

    private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        try
        {
            Disconnected?.Invoke();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MQTT disconnected event failed.");
        }

        return Task.CompletedTask;
    }

    // =========================================================
    // QOS
    // =========================================================

    private static MqttQualityOfServiceLevel ToQos(int qos)
    {
        return qos switch
        {
            0 => MqttQualityOfServiceLevel.AtMostOnce,

            1 => MqttQualityOfServiceLevel.AtLeastOnce,

            2 => MqttQualityOfServiceLevel.ExactlyOnce,

            _ => throw new ArgumentOutOfRangeException(
                nameof(qos),
                qos,
                "MQTT QoS must be 0, 1 or 2."
            ),
        };
    }
}
