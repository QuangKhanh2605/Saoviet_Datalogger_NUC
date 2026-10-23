using System.Text.Json;
using Mqtt.Mqtt.Core;
using Mqtt.Services;

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

        // ========================================================
        // READ MQTT CONFIG
        // GET /api/mqtt/config
        // ========================================================

        app.MapGet(
            "/api/config/mqtt",
            async (MqttConfigFileService configService, CancellationToken cancellationToken) =>
            {
                try
                {
                    string json = await configService.ReadAsync(cancellationToken);

                    return Results.Content(json, "application/json", System.Text.Encoding.UTF8);
                }
                catch (FileNotFoundException ex)
                {
                    return Results.NotFound(new { error = ex.Message });
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    app.Logger.LogError(ex, "Failed to read MQTT configuration.");

                    return Results.Problem("Không thể đọc MqttConfig.json.");
                }
            }
        );

        // ========================================================
        // WRITE MQTT CONFIG AND RESTART
        // PUT /api/mqtt/config
        // ========================================================

        app.MapPut(
            "/api/config/mqtt",
            async (
                JsonElement config,
                MqttConfigFileService configService,
                MqttWorker worker,
                CancellationToken cancellationToken
            ) =>
            {
                try
                {
                    await configService.WriteAsync(config, cancellationToken);

                    // Yêu cầu MqttWorker dừng phiên cũ
                    // và khởi chạy lại.
                    worker.RequestRestart();

                    return Results.Ok(
                        new
                        {
                            success = true,
                            message = "Đã lưu MqttConfig.json và gửi yêu cầu restart MQTT.",
                        }
                    );
                }
                catch (ArgumentException ex)
                {
                    return Results.BadRequest(new { error = ex.Message });
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    app.Logger.LogError(ex, "Failed to update MQTT configuration.");

                    return Results.Problem("Không thể cập nhật cấu hình MQTT.");
                }
            }
        );
    }
}
