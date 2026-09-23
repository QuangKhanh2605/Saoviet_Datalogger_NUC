using Ftp;
using Ftp.Configuration;
using Ftp.Services;
using Ftp.Workers;

var builder = WebApplication.CreateBuilder(args);

// ============================================================
// RUNTIME CONFIGURATION
// ============================================================

builder
    .Configuration.SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(RuntimePaths.AppSettings, optional: false, reloadOnChange: true);

// ============================================================
// APP SETTINGS
// ============================================================

builder.Services.Configure<AppSettings>(builder.Configuration);

// ============================================================
// DATABASE API
// ============================================================

builder.Services.AddHttpClient(
    "DatabaseApi",
    client =>
    {
        string? baseUrl = builder.Configuration["DatabaseApi:BaseUrl"];

        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            throw new InvalidOperationException("DatabaseApi:BaseUrl is not configured.");
        }

        client.BaseAddress = new Uri(baseUrl.EndsWith("/") ? baseUrl : baseUrl + "/");

        client.Timeout = TimeSpan.FromSeconds(10);
    }
);

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

int webPort = builder.Configuration.GetValue<int>("Web:Port");

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
// FTP API
// ============================================================

FtpApi.Map(app);

// ============================================================
// RUN
// ============================================================

app.Run();
