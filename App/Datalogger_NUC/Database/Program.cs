using Database;
using Database.Configuration;
using Database.Services;
using Database.Workers;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// =============================================================
// RUNTIME CONFIGURATION
// =============================================================

builder.Configuration.AddJsonFile(RuntimePaths.AppSettings, optional: false, reloadOnChange: true);

// =============================================================
// APP SETTINGS
// =============================================================

builder.Services.Configure<AppSettings>(builder.Configuration);

// =============================================================
// WEB PORT
// =============================================================

int webPort = builder.Configuration.GetValue<int>("Web:Port");

if (webPort <= 0)
{
    throw new InvalidOperationException("Web:Port is not configured or invalid.");
}

builder.WebHost.UseUrls($"http://0.0.0.0:{webPort}");

// =============================================================
// SERVICES
// =============================================================

builder.Services.AddSingleton<DatabaseService>();

builder.Services.AddSingleton<MessageQueueService>();

// =============================================================
// HTTP CLIENT - MODBUS
// =============================================================

builder.Services.AddHttpClient(
    "Modbus",
    (serviceProvider, client) =>
    {
        AppSettings settings = serviceProvider.GetRequiredService<IOptions<AppSettings>>().Value;

        client.BaseAddress = new Uri(settings.ModbusApi.BaseUrl);

        client.Timeout = TimeSpan.FromSeconds(10);
    }
);

// =============================================================
// WORKER
// =============================================================

builder.Services.AddHostedService<DatabaseWorker>();

// =============================================================
// BUILD
// =============================================================

var app = builder.Build();

// =============================================================
// DATABASE INITIALIZATION
// =============================================================

DatabaseService database = app.Services.GetRequiredService<DatabaseService>();

database.Initialize();

// =============================================================
// API
// =============================================================

DatabaseApi.MapEndpoints(app);

// =============================================================
// RUN
// =============================================================

app.Run();
