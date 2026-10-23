using Mqtt.Configuration;
using Mqtt.Mqtt;
using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Publishing;
using Mqtt.Mqtt.Subscribing;
using Mqtt.Services;
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

string runtimeConfigPath = Path.Combine(AppContext.BaseDirectory, "RuntimeData", "MqttConfig.json");

builder.Configuration.AddJsonFile(runtimeConfigPath, optional: false, reloadOnChange: true);

// Environment variables override JSON configuration.
builder.Configuration.AddEnvironmentVariables();

var runtimeConfig = builder.Configuration;

// ============================================================
// APP SETTINGS
// ============================================================

builder.Services.Configure<AppSettings>(runtimeConfig);

// ============================================================
// WEB PORT
// ============================================================

int webPort = runtimeConfig.GetValue<int>("Web:Port");

if (webPort <= 0 || webPort > 65535)
{
    throw new InvalidOperationException("Web:Port is not configured or invalid.");
}

builder.WebHost.UseUrls($"http://0.0.0.0:{webPort}");

// ============================================================
// REDIS
// ============================================================

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("Mqtt.Redis");

    string connectionString = runtimeConfig.GetConnectionString("Redis") ?? "localhost:6379";

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
// MQTT CONFIG FILE SERVICE
// ============================================================

builder.Services.AddSingleton<MqttConfigFileService>();

// ============================================================
// MQTT CORE
// ============================================================

builder.Services.AddSingleton<MqttService>();
builder.Services.AddSingleton<MqttConnectionManager>();
builder.Services.AddSingleton<MqttTopicBuilder>();

// ============================================================
// MQTT PUBLISH
// ============================================================

builder.Services.AddSingleton<MqttPublisher>();
builder.Services.AddSingleton<MqttPublishManager>();

// ============================================================
// MQTT SUBSCRIBE
// ============================================================

builder.Services.AddSingleton<MqttSubscriber>();

// ============================================================
// MQTT WORKER
// ============================================================

builder.Services.AddSingleton<MqttWorker>();

builder.Services.AddHostedService(serviceProvider =>
    serviceProvider.GetRequiredService<MqttWorker>()
);

// ============================================================
// BUILD
// ============================================================

var app = builder.Build();

// ============================================================
// STARTUP LOGGING
// ============================================================

var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Mqtt.Startup");

logger.LogInformation("MQTT service is starting on port {WebPort}.", webPort);

// ============================================================
// API
// ============================================================

MqttApi.Map(app);

// ============================================================
// RUN
// ============================================================

app.Run();
