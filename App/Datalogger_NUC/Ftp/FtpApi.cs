using Ftp.Workers;

namespace Ftp;

public static class FtpApi
{
    public static void Map(WebApplication app)
    {
        // ========================================================
        // HEALTH
        // ========================================================

        app.MapGet(
            "/health",
            (FtpWorker worker) =>
            {
                return Results.Ok(worker.GetHealthStatus());
            }
        );

        // ========================================================
        // STATUS
        // ========================================================

        app.MapGet(
            "/status",
            (FtpWorker worker) =>
            {
                return Results.Ok(worker.GetStatus());
            }
        );

        // ========================================================
        // ROOT
        // ========================================================

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
                    }
                );
            }
        );
    }
}
