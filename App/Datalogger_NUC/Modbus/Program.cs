using Modbus;
using Modbus.Configuration;
using Modbus.Decoders;
using Modbus.Sensors;
using Modbus.Services;
using Modbus.Workers;
using StackExchange.Redis;

// ============================================================
// CONFIGURATION
// ============================================================

var builder = WebApplication.CreateBuilder(args);

// ============================================================
// APPSETTINGS - LOGGING
// ============================================================

builder
    .Configuration.SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

// ============================================================
// RUNTIME CONFIGURATION
// ============================================================

builder.Configuration.AddJsonFile(RuntimePaths.AppSettings, optional: false, reloadOnChange: true);

// Environment variables override JSON configuration.
builder.Configuration.AddEnvironmentVariables();

// ============================================================
// APP SETTINGS
// ============================================================

builder.Services.Configure<AppSettings>(builder.Configuration);

// ============================================================
// WEB PORT
// ============================================================

int webPort = builder.Configuration.GetValue<int>("Web:Port");

if (webPort <= 0 || webPort > 65535)
{
    throw new InvalidOperationException($"Web:Port không hợp lệ: {webPort}");
}

builder.WebHost.UseUrls($"http://0.0.0.0:{webPort}");

// ============================================================
// REDIS
// ============================================================

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("Modbus.Redis");

    string connectionString =
        builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";

    ConfigurationOptions redisOptions = ConfigurationOptions.Parse(connectionString);

    redisOptions.AbortOnConnectFail = false;
    redisOptions.ConnectTimeout = 3000;
    redisOptions.ConnectRetry = 1;
    redisOptions.ReconnectRetryPolicy = new ExponentialRetry(3000);

    logger.LogInformation("Initializing Redis connection to {RedisEndpoint}.", connectionString);

    try
    {
        return ConnectionMultiplexer.Connect(redisOptions);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Redis initialization failed for {RedisEndpoint}.", connectionString);

        throw;
    }
});

// ============================================================
// MODBUS SERVICES
// ============================================================

builder.Services.AddSingleton<ReadingStore>();

builder.Services.AddSingleton<DecoderFactory>();

builder.Services.AddSingleton<StationConfigurationService>(_ => new StationConfigurationService(
    RuntimePaths.Stations
));

builder.Services.AddSingleton<StationReader>();

// ============================================================
// MODBUS WORKER
// ============================================================

builder.Services.AddSingleton<ModbusWorker>();

builder.Services.AddHostedService(serviceProvider =>
    serviceProvider.GetRequiredService<ModbusWorker>()
);

// ============================================================
// BUILD
// ============================================================

var app = builder.Build();

// ============================================================
// STARTUP LOGGING
// ============================================================

var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Modbus.Startup");

logger.LogInformation("Modbus service is starting on port {WebPort}.", webPort);

// ============================================================
// HTTP API
// ============================================================

ModbusApi.Map(app);

// ============================================================
// RUN
// ============================================================

app.Run();
