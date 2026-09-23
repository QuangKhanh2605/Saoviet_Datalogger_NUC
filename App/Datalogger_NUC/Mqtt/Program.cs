using Mqtt.Configuration;
using Mqtt.Mqtt;
using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Publishing;
using Mqtt.Mqtt.Subscribing;

var builder = WebApplication.CreateBuilder(args);

// ============================================================
// RUNTIME DATA CONFIGURATION
// ============================================================

builder
    .Configuration.SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile(
        Path.Combine("RuntimeData", "MqttConfig.json"),
        optional: false,
        reloadOnChange: true
    );

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

builder.Services.AddHostedService<MqttWorker>();

// ============================================================
// BUILD
// ============================================================

var app = builder.Build();

// ============================================================
// API
// ============================================================

MqttApi.Map(app);

// ============================================================
// RUN
// ============================================================

app.Run();
