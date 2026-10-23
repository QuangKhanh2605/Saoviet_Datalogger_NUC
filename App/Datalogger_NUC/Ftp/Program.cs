using Ftp;
using Ftp.Configuration;
using Ftp.Services;
using Ftp.Workers;
using Microsoft.Extensions.Configuration;
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
// FTP RUNTIME CONFIGURATION
// ============================================================

string ftpConfigPath = Path.Combine(AppContext.BaseDirectory, "RuntimeData", "FtpConfig.json");

builder.Configuration.AddJsonFile(ftpConfigPath, optional: false, reloadOnChange: true);

// Environment variables override JSON configuration.
builder.Configuration.AddEnvironmentVariables();

var ftpConfig = builder.Configuration;

builder.Services.Configure<AppSettings>(ftpConfig);

// ============================================================
// REDIS
// ============================================================

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("Ftp.Redis");

    string connectionString = ftpConfig.GetConnectionString("Redis") ?? "localhost:6379";

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
        logger.LogError(
            ex,
            "Redis connection initialization failed for {RedisEndpoint}.",
            connectionString
        );

        throw;
    }
});

// ============================================================
// CONFIGURATION FILE SERVICE
// ============================================================

builder.Services.AddSingleton<FtpConfigFileService>();

// ============================================================
// SERVICES
// ============================================================

builder.Services.AddSingleton<DatabaseApiService>();

builder.Services.AddSingleton<FtpService>();

// ============================================================
// FTP WORKER
// ============================================================

builder.Services.AddSingleton<FtpWorker>();

builder.Services.AddHostedService(provider => provider.GetRequiredService<FtpWorker>());

// ============================================================
// WEB PORT
// ============================================================

int webPort = ftpConfig.GetValue<int>("Web:Port");

if (webPort <= 0 || webPort > 65535)
{
    throw new InvalidOperationException($"Web:Port không hợp lệ: {webPort}");
}

builder.WebHost.UseUrls($"http://0.0.0.0:{webPort}");

// ============================================================
// BUILD
// ============================================================

var app = builder.Build();

// ============================================================
// STARTUP LOGGING
// ============================================================

var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Ftp.Startup");

logger.LogInformation("FTP service is starting on port {WebPort}.", webPort);

// ============================================================
// FTP API
// ============================================================

FtpApi.Map(app);

// ============================================================
// RUN
// ============================================================

app.Run();
