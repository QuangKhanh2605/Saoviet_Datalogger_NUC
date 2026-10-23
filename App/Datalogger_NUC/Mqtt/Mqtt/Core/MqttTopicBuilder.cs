using Microsoft.Extensions.Options;
using Mqtt.Configuration;

namespace Mqtt.Mqtt.Core;

public sealed class MqttTopicBuilder
{
    private readonly IOptionsMonitor<AppSettings> _settings;

    public MqttTopicBuilder(IOptionsMonitor<AppSettings> options)
    {
        _settings = options;
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

        // Luôn đọc cấu hình mới nhất.
        AppSettings settings = _settings.CurrentValue;

        if (string.IsNullOrWhiteSpace(settings.Mqtt.Topic))
        {
            throw new InvalidOperationException("Mqtt.Topic is not configured.");
        }

        if (string.IsNullOrWhiteSpace(settings.Device.Id))
        {
            throw new InvalidOperationException("Device.Id is not configured.");
        }

        string prefix = settings.Mqtt.Topic.Trim('/');
        string topic = logicalTopic.Trim('/');
        string deviceId = settings.Device.Id.Trim('/');

        string result = $"{prefix}/{topic}/{deviceId}";

        return result;
    }

    public string BuildSubscribeTopic()
    {
        // Luôn đọc cấu hình mới nhất.
        AppSettings settings = _settings.CurrentValue;

        if (string.IsNullOrWhiteSpace(settings.Mqtt.Sub))
        {
            throw new InvalidOperationException("Mqtt.Sub is not configured.");
        }

        if (string.IsNullOrWhiteSpace(settings.Device.Id))
        {
            throw new InvalidOperationException("Device.Id is not configured.");
        }

        string prefix = settings.Mqtt.Sub.Trim('/');
        string deviceId = settings.Device.Id.Trim('/');

        return $"{prefix}/{deviceId}/#";
    }
}
