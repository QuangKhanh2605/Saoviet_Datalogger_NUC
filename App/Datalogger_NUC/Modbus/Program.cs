using Modbus;
using Modbus.Configuration;
using Modbus.Decoders;
using Modbus.Sensors;
using Modbus.Services;
using Modbus.Workers;

var builder = WebApplication.CreateBuilder(args);

// ============================================================
// CONFIGURATION
// ============================================================

builder
    .Configuration.SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(RuntimePaths.AppSettings, optional: false, reloadOnChange: true);

builder.Services.Configure<AppSettings>(builder.Configuration);

// ============================================================
// WEB PORT
// ============================================================

int webPort = builder.Configuration.GetValue<int>("Web:Port");

if (webPort <= 0)
{
    throw new InvalidOperationException("Web:Port is not configured or invalid.");
}

builder.WebHost.UseUrls($"http://0.0.0.0:{webPort}");

// ============================================================
// MODBUS SERVICES
// ============================================================

builder.Services.AddSingleton<ReadingStore>();

builder.Services.AddSingleton<DecoderFactory>();

builder.Services.AddSingleton<StationConfigurationService>(serviceProvider =>
{
    return new StationConfigurationService(RuntimePaths.Stations);
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

ModbusApi.Map(app);

// ============================================================
// RUN
// ============================================================

app.Run();
