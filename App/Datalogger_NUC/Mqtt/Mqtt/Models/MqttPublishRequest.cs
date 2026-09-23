namespace Mqtt.Mqtt.Models;

public sealed class MqttPublishRequest
{
    public string Topic { get; init; } = "";

    public string Payload { get; init; } = "";

    public Func<Task>? OnSuccess { get; init; }
}
