using Mqtt.Mqtt.Core;

namespace Mqtt.Mqtt;

public static class MqttApi
{
    public static void Map(WebApplication app)
    {
        // ========================================================
        // HEALTH
        // ========================================================

        app.MapGet(
            "/health",
            () =>
            {
                return Results.Ok(new { service = "Mqtt", status = "healthy" });
            }
        );

        // ========================================================
        // ROOT
        // ========================================================

        app.MapGet(
            "/",
            () =>
            {
                return Results.Ok(new { service = "Mqtt", status = "running" });
            }
        );

        // ========================================================
        // MQTT STATUS
        // ========================================================

        app.MapGet(
            "/api/mqtt/status",
            (MqttConnectionManager mqtt) =>
            {
                return Results.Ok(
                    new
                    {
                        service = "Mqtt",
                        connected = mqtt.IsConnected,
                        ready = mqtt.IsReady,
                        server = mqtt.CurrentServerName,
                        ip = mqtt.CurrentServerIp,
                    }
                );
            }
        );
    }
}
