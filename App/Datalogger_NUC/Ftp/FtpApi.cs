using System.Text;
using System.Text.Json;
using Ftp.Services;
using Ftp.Workers;

namespace Ftp;

public static class FtpApi
{
    public static void Map(WebApplication app)
    {
        // ============================================================
        // HEALTH
        // ============================================================

        app.MapGet(
            "/health",
            (FtpWorker worker) =>
            {
                return Results.Ok(worker.GetHealthStatus());
            }
        );

        // ============================================================
        // STATUS
        // ============================================================

        app.MapGet(
            "/status",
            (FtpWorker worker) =>
            {
                return Results.Ok(worker.GetStatus());
            }
        );

        // ============================================================
        // ROOT
        // ============================================================

        app.MapGet(
            "/",
            () =>
            {
                return Results.Ok(
                    new
                    {
                        service = "FTP",
                        status = "Running",
                        health = "/health",
                        detail = "/status",
                        config = "/api/ftp/config",
                    }
                );
            }
        );

        // ============================================================
        // GET FTP CONFIG
        // Trả về JSON có định dạng xuống dòng, thụt lề.
        // ============================================================

        app.MapGet(
            "/api/config/ftp",
            async (FtpConfigFileService configService, CancellationToken cancellationToken) =>
            {
                try
                {
                    JsonElement config = await configService.ReadAsync(cancellationToken);

                    string json = JsonSerializer.Serialize(
                        config,
                        new JsonSerializerOptions { WriteIndented = true }
                    );

                    return Results.Content(json, "application/json", Encoding.UTF8);
                }
                catch (Exception ex)
                {
                    return Results.Problem(
                        title: "Không thể đọc cấu hình FTP.",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status500InternalServerError
                    );
                }
            }
        );

        // ============================================================
        // PUT FTP CONFIG
        // Lưu cấu hình mới và yêu cầu khởi động lại worker.
        // ============================================================

        app.MapPut(
            "/api/config/ftp",
            async (
                JsonElement config,
                FtpConfigFileService configService,
                FtpWorker worker,
                CancellationToken cancellationToken
            ) =>
            {
                try
                {
                    if (config.ValueKind != JsonValueKind.Object)
                    {
                        return Results.BadRequest(
                            new { message = "Cấu hình FTP phải là một JSON object." }
                        );
                    }

                    await configService.WriteAsync(config, cancellationToken);

                    // Yêu cầu worker kết thúc phiên hiện tại
                    // và chạy lại với cấu hình mới.
                    worker.RequestRestart();

                    return Results.Ok(
                        new
                        {
                            service = "FTP",
                            status = "RestartRequested",
                            message = "Đã lưu cấu hình FTP và yêu cầu khởi động lại worker.",
                        }
                    );
                }
                catch (JsonException ex)
                {
                    return Results.BadRequest(
                        new { message = "JSON cấu hình không hợp lệ.", detail = ex.Message }
                    );
                }
                catch (Exception ex)
                {
                    return Results.Problem(
                        title: "Không thể cập nhật cấu hình FTP.",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status500InternalServerError
                    );
                }
            }
        );
    }
}
