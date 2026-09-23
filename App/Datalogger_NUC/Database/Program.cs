using Database.Configuration;
using Database.Models;
using Database.Services;
using Database.Workers;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// =============================================================
// RUNTIME CONFIGURATION
// =============================================================

builder.Configuration.AddJsonFile(
    RuntimePaths.AppSettings,
    optional: false,
    reloadOnChange: true);

// =============================================================
// APP SETTINGS
// =============================================================

builder.Services.Configure<AppSettings>(
    builder.Configuration);

// =============================================================
// WEB PORT
// =============================================================

int webPort =
    builder.Configuration.GetValue<int>("Web:Port");

if (webPort <= 0)
{
    throw new InvalidOperationException(
        "Web:Port is not configured or invalid.");
}

builder.WebHost.UseUrls(
    $"http://0.0.0.0:{webPort}");

// =============================================================
// SERVICES
// =============================================================

builder.Services.AddSingleton<DatabaseService>();

// =============================================================
// HTTP CLIENT - MODBUS
// =============================================================

builder.Services.AddHttpClient(
    "Modbus",
    (serviceProvider, client) =>
    {
        AppSettings settings =
            serviceProvider
                .GetRequiredService<IOptions<AppSettings>>()
                .Value;

        client.BaseAddress =
            new Uri(settings.ModbusApi.BaseUrl);

        client.Timeout =
            TimeSpan.FromSeconds(10);
    });

// =============================================================
// WORKER
// =============================================================

builder.Services.AddHostedService<DatabaseWorker>();

// =============================================================
// BUILD APPLICATION
// =============================================================

var app = builder.Build();

// =============================================================
// DATABASE INITIALIZATION
// =============================================================

DatabaseService database =
    app.Services.GetRequiredService<DatabaseService>();

database.Initialize();

// =============================================================
// BASIC API
// =============================================================

app.MapGet(
    "/",
    () =>
    {
        return Results.Ok(new
        {
            service = "Database",
            status = "running"
        });
    });

app.MapGet(
    "/health",
    () =>
    {
        return Results.Ok(new
        {
            service = "Database",
            status = "healthy"
        });
    });

// =============================================================
// MQTT
// GET OLDEST UNSENT MQTT MESSAGES
// =============================================================

app.MapGet(
    "/api/mqtt/pending",
    (
        DatabaseService databaseService,
        int? maxMessages) =>
    {
        int max =
            maxMessages.GetValueOrDefault(10);

        if (max <= 0)
        {
            max = 10;
        }

        if (max > 500)
        {
            max = 500;
        }

        List<StationMessage> messages =
            databaseService.GetPendingMqttMessages(max);

        return Results.Ok(messages);
    });

// =============================================================
// MQTT
// MARK SUCCESSFULLY SENT MESSAGES
// =============================================================

app.MapPost(
    "/api/mqtt/mark-sent",
    (
        DatabaseService databaseService,
        MarkMessagesSentRequest request) =>
    {
        if (request.Ids == null ||
            request.Ids.Count == 0)
        {
            return Results.BadRequest(new
            {
                error = "Ids cannot be empty."
            });
        }

        databaseService.MarkMqttSent(
            request.Ids);

        return Results.Ok(new
        {
            success = true,
            type = "mqtt",
            markedCount = request.Ids.Count,
            ids = request.Ids
        });
    });

// =============================================================
// FTP
// GET OLDEST UNSENT FTP MESSAGES
// =============================================================

app.MapGet(
    "/api/ftp/pending",
    (
        DatabaseService databaseService,
        string? stationNames,
        int? maxMessages) =>
    {
        int max =
            maxMessages.GetValueOrDefault(10);

        if (max <= 0)
        {
            max = 10;
        }

        if (max > 500)
        {
            max = 500;
        }

        List<string>? stations = null;

        if (!string.IsNullOrWhiteSpace(stationNames))
        {
            stations =
                stationNames
                    .Split(
                        ',',
                        StringSplitOptions.RemoveEmptyEntries |
                        StringSplitOptions.TrimEntries)
                    .ToList();
        }

        List<StationMessage> messages =
            databaseService.GetPendingFtpMessages(
                stations,
                max);

        return Results.Ok(messages);
    });

// =============================================================
// FTP
// MARK SUCCESSFULLY SENT MESSAGES
// =============================================================

app.MapPost(
    "/api/ftp/mark-sent",
    (
        DatabaseService databaseService,
        MarkMessagesSentRequest request) =>
    {
        if (request.Ids == null ||
            request.Ids.Count == 0)
        {
            return Results.BadRequest(new
            {
                error = "Ids cannot be empty."
            });
        }

        databaseService.MarkFtpSent(
            request.Ids);

        return Results.Ok(new
        {
            success = true,
            type = "ftp",
            markedCount = request.Ids.Count,
            ids = request.Ids
        });
    });

// =============================================================
// FTP2
// GET OLDEST UNSENT FTP2 MESSAGES
// =============================================================

app.MapGet(
    "/api/ftp2/pending",
    (
        DatabaseService databaseService,
        string? stationNames,
        int? maxMessages) =>
    {
        int max =
            maxMessages.GetValueOrDefault(10);

        if (max <= 0)
        {
            max = 10;
        }

        if (max > 500)
        {
            max = 500;
        }

        List<string>? stations = null;

        if (!string.IsNullOrWhiteSpace(stationNames))
        {
            stations =
                stationNames
                    .Split(
                        ',',
                        StringSplitOptions.RemoveEmptyEntries |
                        StringSplitOptions.TrimEntries)
                    .ToList();
        }

        List<StationMessage> messages =
            databaseService.GetPendingFtp2Messages(
                stations,
                max);

        return Results.Ok(messages);
    });

// =============================================================
// FTP2
// MARK SUCCESSFULLY SENT MESSAGES
// =============================================================

app.MapPost(
    "/api/ftp2/mark-sent",
    (
        DatabaseService databaseService,
        MarkMessagesSentRequest request) =>
    {
        if (request.Ids == null ||
            request.Ids.Count == 0)
        {
            return Results.BadRequest(new
            {
                error = "Ids cannot be empty."
            });
        }

        databaseService.MarkFtp2Sent(
            request.Ids);

        return Results.Ok(new
        {
            success = true,
            type = "ftp2",
            markedCount = request.Ids.Count,
            ids = request.Ids
        });
    });

// =============================================================
// DEBUG - SHOW ALL PENDING MQTT MESSAGES
// =============================================================

app.MapGet(
    "/api/messages",
    (
        DatabaseService databaseService,
        int? max) =>
    {
        int maxMessages =
            max.GetValueOrDefault(50);

        if (maxMessages <= 0)
        {
            maxMessages = 50;
        }

        if (maxMessages > 500)
        {
            maxMessages = 500;
        }

        List<StationMessage> messages =
            databaseService.GetPendingMqttMessages(
                maxMessages);

        return Results.Ok(messages);
    });

// =============================================================
// RUN
// =============================================================

app.Run();