using Modbus.Configuration;
using Modbus.Decoders;
using Modbus.Services;
using Modbus.Sensors;
using Modbus.Workers;

var builder = WebApplication.CreateBuilder(args);

// ============================================================
// CONFIGURATION
// ============================================================

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(
        RuntimePaths.AppSettings,
        optional: false,
        reloadOnChange: true);

builder.Services.Configure<AppSettings>(
    builder.Configuration);

// ============================================================
// WEB PORT
// ============================================================

int webPort =
    builder.Configuration.GetValue<int>("Web:Port");

if (webPort <= 0)
{
    throw new InvalidOperationException(
        "Web:Port is not configured or invalid.");
}

builder.WebHost.UseUrls(
    $"http://0.0.0.0:{webPort}");

// ============================================================
// MODBUS SERVICES
// ============================================================

builder.Services.AddSingleton<ReadingStore>();

builder.Services.AddSingleton<DecoderFactory>();

builder.Services.AddSingleton<StationConfigurationService>(
    serviceProvider =>
    {
        return new StationConfigurationService(
            RuntimePaths.Stations);
    });

builder.Services.AddSingleton<StationReader>();

builder.Services.AddHostedService<ModbusWorker>();

// ============================================================
// BUILD
// ============================================================

var app = builder.Build();

// ============================================================
// HTTP API
// ============================================================

// Lấy toàn bộ dữ liệu cảm biến mới nhất trong RAM.
app.MapGet(
    "/api/readings",
    (ReadingStore readingStore) =>
    {
        return Results.Ok(
            readingStore.GetLatestReadings());
    });

// Lấy dữ liệu mới nhất của một trạm.
app.MapGet(
    "/api/readings/{stationName}",
    (
        string stationName,
        ReadingStore readingStore) =>
    {
        var readings = readingStore
            .GetLatestReadings()
            .Where(x =>
                string.Equals(
                    x.StationName,
                    stationName,
                    StringComparison.OrdinalIgnoreCase))
            .ToList();

        return Results.Ok(readings);
    });

// Kiểm tra Modbus service đang chạy.
app.MapGet(
    "/api/health",
    () =>
    {
        return Results.Ok(new
        {
            Service = "Modbus",
            Status = "Running"
        });
    });

// ============================================================
// RUN
// ============================================================

app.Run();