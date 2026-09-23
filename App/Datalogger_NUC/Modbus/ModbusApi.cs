using Modbus.Services;

namespace Modbus;

public static class ModbusApi
{
    public static void Map(WebApplication app)
    {
        // ========================================================
        // GET ALL READINGS
        // ========================================================

        app.MapGet(
            "/api/readings",
            (ReadingStore readingStore) =>
            {
                return Results.Ok(readingStore.GetLatestReadings());
            }
        );

        // ========================================================
        // GET READINGS BY STATION
        // ========================================================

        app.MapGet(
            "/api/readings/{stationName}",
            (string stationName, ReadingStore readingStore) =>
            {
                var readings = readingStore
                    .GetLatestReadings()
                    .Where(x =>
                        string.Equals(
                            x.StationName,
                            stationName,
                            StringComparison.OrdinalIgnoreCase
                        )
                    )
                    .ToList();

                return Results.Ok(readings);
            }
        );

        // ========================================================
        // HEALTH
        // ========================================================

        app.MapGet(
            "/api/health",
            () =>
            {
                return Results.Ok(new { Service = "Modbus", Status = "Running" });
            }
        );
    }
}
