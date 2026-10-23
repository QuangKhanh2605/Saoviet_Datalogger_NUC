using Database;
using Database.Configuration;
using Database.Services;
using Database.Workers;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// =============================================================
// APPSETTINGS - LOGGING
// =============================================================

builder
    .Configuration.SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

// =============================================================
// RUNTIME CONFIGURATION
// =============================================================

var runtimeConfig = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(RuntimePaths.AppSettings, optional: false, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

// =============================================================
// APP SETTINGS
// =============================================================

builder.Services.AddSingleton<IConfigurationRoot>(runtimeConfig);

builder.Services.Configure<AppSettings>(runtimeConfig);

// =============================================================
// WEB PORT
// =============================================================

int webPort = runtimeConfig.GetValue<int>("Web:Port");

if (webPort <= 0 || webPort > 65535)
{
    throw new InvalidOperationException("Web:Port is not configured or invalid.");
}

builder.WebHost.UseUrls($"http://0.0.0.0:{webPort}");

// =============================================================
// REDIS
// =============================================================

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var logger = serviceProvider
        .GetRequiredService<ILoggerFactory>()
        .CreateLogger("Database.Redis");

    string connectionString = runtimeConfig.GetConnectionString("Redis") ?? "localhost:6379";

    ConfigurationOptions redisOptions;

    try
    {
        redisOptions = ConfigurationOptions.Parse(connectionString);

        redisOptions.AbortOnConnectFail = false;
        redisOptions.ConnectTimeout = 3000;
        redisOptions.ConnectRetry = 1;
        redisOptions.ReconnectRetryPolicy = new ExponentialRetry(3000);

        logger.LogInformation(
            "Initializing Redis connection to {RedisEndpoint}.",
            connectionString
        );

        return ConnectionMultiplexer.Connect(redisOptions);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Redis initialization failed for {RedisEndpoint}.", connectionString);

        throw;
    }
});

// =============================================================
// SERVICES
// =============================================================

builder.Services.AddSingleton<DatabaseService>();

builder.Services.AddSingleton<MessageQueueService>();

builder.Services.AddSingleton<DatabaseConfigFileService>();

// =============================================================
// WORKER
// =============================================================

builder.Services.AddSingleton<DatabaseWorker>();

builder.Services.AddHostedService(serviceProvider =>
    serviceProvider.GetRequiredService<DatabaseWorker>()
);

// =============================================================
// BUILD
// =============================================================

var app = builder.Build();

// =============================================================
// STARTUP LOGGING
// =============================================================

var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Database.Startup");

// =============================================================
// SQLITE INITIALIZATION
// =============================================================

var database = app.Services.GetRequiredService<DatabaseService>();

try
{
    database.Initialize();

    logger.LogInformation("SQLite database initialized successfully.");
}
catch (Exception ex)
{
    logger.LogError(
        ex,
        "SQLite initialization failed. The application will continue starting; DatabaseWorker must retry initialization."
    );
}

// =============================================================
// API
// =============================================================

DatabaseApi.MapEndpoints(app);

// =============================================================
// RUN
// =============================================================

logger.LogInformation("Database API is starting on port {WebPort}.", webPort);

app.Run();
