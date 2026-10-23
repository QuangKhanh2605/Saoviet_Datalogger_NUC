using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Models;

namespace Mqtt.Mqtt.Publishing;

public sealed class MqttPublisher
{
    private readonly MqttService _mqtt;
    private readonly MqttConnectionManager _connection;
    private readonly MqttTopicBuilder _topicBuilder;

    public MqttPublisher(
        MqttService mqtt,
        MqttConnectionManager connection,
        MqttTopicBuilder topicBuilder
    )
    {
        _mqtt = mqtt;
        _connection = connection;
        _topicBuilder = topicBuilder;
    }

    public async Task<bool> PublishAsync(
        MqttPublishRequest request,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!_connection.IsReady)
        {
            return false;
        }

        string fullTopic = _topicBuilder.Build(request.Topic);

        try
        {
            await _mqtt.PublishAsync(fullTopic, request.Payload, cancellationToken);

            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            return false;
        }
    }
}
