using Microsoft.Extensions.Options;
using Mqtt.Configuration;

namespace Mqtt.Mqtt.Core;

public sealed class MqttTopicBuilder
{
    private readonly AppSettings _settings;

    public MqttTopicBuilder(IOptions<AppSettings> options)
    {
        _settings = options.Value;
    }

    public string Build(string logicalTopic)
    {
        if (string.IsNullOrWhiteSpace(logicalTopic))
        {
            throw new ArgumentException(
                "MQTT logical topic cannot be empty.",
                nameof(logicalTopic)
            );
        }

        if (string.IsNullOrWhiteSpace(_settings.Mqtt.Topic))
        {
            throw new InvalidOperationException("Mqtt.Topic is not configured.");
        }

        if (string.IsNullOrWhiteSpace(_settings.Device.Id))
        {
            throw new InvalidOperationException("Device.Id is not configured.");
        }

        string prefix = _settings.Mqtt.Topic.Trim('/');

        string topic = logicalTopic.Trim('/');

        string deviceId = _settings.Device.Id.Trim('/');

        return $"{prefix}/{topic}/{deviceId}";
    }

    public string BuildSubscribeTopic()
    {
        if (string.IsNullOrWhiteSpace(_settings.Mqtt.Sub))
        {
            throw new InvalidOperationException("Mqtt.Sub is not configured.");
        }

        if (string.IsNullOrWhiteSpace(_settings.Device.Id))
        {
            throw new InvalidOperationException("Device.Id is not configured.");
        }

        string prefix = _settings.Mqtt.Sub.Trim('/');

        string deviceId = _settings.Device.Id.Trim('/');

        return $"{prefix}/{deviceId}/#";
    }
}
