using System.Text;
using System.Text.Json;
using Database.Configuration;
using Database.Models;
using Database.Services;
using Database.Workers;

namespace Database;

public static class DatabaseApi
{
    public static void MapEndpoints(WebApplication app)
    {
        // =============================================================
        // BASIC API
        // =============================================================

        app.MapGet(
            "/",
            () =>
            {
                return Results.Ok(new { service = "Database", status = "running" });
            }
        );

        app.MapGet(
            "/health",
            () =>
            {
                return Results.Ok(new { service = "Database", status = "healthy" });
            }
        );

        // =============================================================
        // CONFIG - GET DATABASE CONFIGURATION
        // GET /api/config/database
        // =============================================================

        app.MapGet(
            "/api/config/database",
            (DatabaseConfigFileService configService) =>
            {
                try
                {
                    string json = configService.ReadConfig();

                    return Results.Content(json, "application/json", Encoding.UTF8);
                }
                catch (FileNotFoundException ex)
                {
                    return Results.Problem(
                        title: "Configuration file not found.",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound
                    );
                }
                catch (Exception ex)
                {
                    return Results.Problem(
                        title: "Failed to read database configuration.",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status500InternalServerError
                    );
                }
            }
        );

        // =============================================================
        // CONFIG - UPDATE DATABASE CONFIGURATION
        // PUT /api/config/database
        // =============================================================

        app.MapPut(
            "/api/config/database",
            async (
                HttpRequest request,
                DatabaseConfigFileService configService,
                DatabaseWorker worker,
                CancellationToken cancellationToken
            ) =>
            {
                try
                {
                    using JsonDocument document = await JsonDocument.ParseAsync(
                        request.Body,
                        cancellationToken: cancellationToken
                    );

                    if (document.RootElement.ValueKind != JsonValueKind.Object)
                    {
                        return Results.BadRequest(
                            new { error = "Configuration must be a JSON object." }
                        );
                    }

                    string json = JsonSerializer.Serialize(
                        document.RootElement,
                        new JsonSerializerOptions { WriteIndented = true }
                    );

                    // Validate and save the new configuration.
                    configService.WriteConfig(json);

                    // Ask DatabaseWorker to recalculate its schedules.
                    worker.RequestRestart();

                    return Results.Ok(
                        new
                        {
                            success = true,
                            message = "DatabaseConfig.json updated. Worker configuration reload requested.",
                        }
                    );
                }
                catch (JsonException ex)
                {
                    return Results.BadRequest(
                        new { error = "Invalid JSON configuration.", detail = ex.Message }
                    );
                }
                catch (ArgumentException ex)
                {
                    return Results.BadRequest(
                        new { error = "Invalid database configuration.", detail = ex.Message }
                    );
                }
                catch (InvalidOperationException ex)
                {
                    return Results.BadRequest(
                        new { error = "Invalid database configuration.", detail = ex.Message }
                    );
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return Results.StatusCode(499);
                }
                catch (Exception ex)
                {
                    return Results.Problem(
                        title: "Failed to update database configuration.",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status500InternalServerError
                    );
                }
            }
        );

        //     // =============================================================
        //     // MQTT - GET PENDING
        //     // =============================================================

        //     app.MapGet(
        //         "/api/mqtt/pending",
        //         (MessageQueueService queueService, int? maxMessages) =>
        //         {
        //             try
        //             {
        //                 int max = maxMessages.GetValueOrDefault(10);

        //                 if (max <= 0)
        //                 {
        //                     max = 10;
        //                 }

        //                 if (max > 100)
        //                 {
        //                     max = 100;
        //                 }

        //                 List<StationMessage> messages = queueService.GetPendingMqttMessages(max);

        //                 return Results.Ok(messages.Select(ToApiMessage));
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );

        //     // =============================================================
        //     // MQTT - MARK SENT
        //     // =============================================================

        //     app.MapPost(
        //         "/api/mqtt/mark-sent",
        //         (MessageQueueService queueService, MarkMessagesSentRequest request) =>
        //         {
        //             if (request.Ids == null || request.Ids.Count == 0)
        //             {
        //                 return Results.BadRequest(new { error = "Ids cannot be empty." });
        //             }

        //             try
        //             {
        //                 int markedCount = queueService.MarkMqttSent(request.Ids);

        //                 return Results.Ok(
        //                     new
        //                     {
        //                         success = true,
        //                         type = "mqtt",
        //                         markedCount,
        //                         ids = request.Ids,
        //                     }
        //                 );
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );

        //     // =============================================================
        //     // FTP - GET PENDING
        //     // =============================================================

        //     app.MapGet(
        //         "/api/ftp/pending",
        //         (MessageQueueService queueService, string? stationNames, int? maxMessages) =>
        //         {
        //             try
        //             {
        //                 int max = maxMessages.GetValueOrDefault(10);

        //                 if (max <= 0)
        //                 {
        //                     max = 10;
        //                 }

        //                 if (max > 100)
        //                 {
        //                     max = 100;
        //                 }

        //                 List<string>? stations = null;

        //                 if (!string.IsNullOrWhiteSpace(stationNames))
        //                 {
        //                     stations = stationNames
        //                         .Split(
        //                             ',',
        //                             StringSplitOptions.RemoveEmptyEntries
        //                                 | StringSplitOptions.TrimEntries
        //                         )
        //                         .ToList();
        //                 }

        //                 List<StationMessage> messages = queueService.GetPendingFtpMessages(
        //                     stations,
        //                     max
        //                 );

        //                 return Results.Ok(messages.Select(ToApiMessage));
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );

        //     // =============================================================
        //     // FTP - MARK SENT
        //     // =============================================================

        //     app.MapPost(
        //         "/api/ftp/mark-sent",
        //         (MessageQueueService queueService, MarkMessagesSentRequest request) =>
        //         {
        //             if (request.Ids == null || request.Ids.Count == 0)
        //             {
        //                 return Results.BadRequest(new { error = "Ids cannot be empty." });
        //             }

        //             try
        //             {
        //                 int markedCount = queueService.MarkFtpSent(request.Ids);

        //                 return Results.Ok(
        //                     new
        //                     {
        //                         success = true,
        //                         type = "ftp",
        //                         markedCount,
        //                         ids = request.Ids,
        //                     }
        //                 );
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );

        //     // =============================================================
        //     // FTP2 - GET PENDING
        //     // =============================================================

        //     app.MapGet(
        //         "/api/ftp2/pending",
        //         (MessageQueueService queueService, string? stationNames, int? maxMessages) =>
        //         {
        //             try
        //             {
        //                 int max = maxMessages.GetValueOrDefault(10);

        //                 if (max <= 0)
        //                 {
        //                     max = 10;
        //                 }

        //                 if (max > 100)
        //                 {
        //                     max = 100;
        //                 }

        //                 List<string>? stations = null;

        //                 if (!string.IsNullOrWhiteSpace(stationNames))
        //                 {
        //                     stations = stationNames
        //                         .Split(
        //                             ',',
        //                             StringSplitOptions.RemoveEmptyEntries
        //                                 | StringSplitOptions.TrimEntries
        //                         )
        //                         .ToList();
        //                 }

        //                 List<StationMessage> messages = queueService.GetPendingFtp2Messages(
        //                     stations,
        //                     max
        //                 );

        //                 return Results.Ok(messages.Select(ToApiMessage));
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );

        //     // =============================================================
        //     // FTP2 - MARK SENT
        //     // =============================================================

        //     app.MapPost(
        //         "/api/ftp2/mark-sent",
        //         (MessageQueueService queueService, MarkMessagesSentRequest request) =>
        //         {
        //             if (request.Ids == null || request.Ids.Count == 0)
        //             {
        //                 return Results.BadRequest(new { error = "Ids cannot be empty." });
        //             }

        //             try
        //             {
        //                 int markedCount = queueService.MarkFtp2Sent(request.Ids);

        //                 return Results.Ok(
        //                     new
        //                     {
        //                         success = true,
        //                         type = "ftp2",
        //                         markedCount,
        //                         ids = request.Ids,
        //                     }
        //                 );
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );

        //     // =============================================================
        //     // DEBUG - MQTT QUEUE
        //     // =============================================================

        //     app.MapGet(
        //         "/api/messages",
        //         (MessageQueueService queueService, int? max) =>
        //         {
        //             try
        //             {
        //                 int maxMessages = max.GetValueOrDefault(50);

        //                 if (maxMessages <= 0)
        //                 {
        //                     maxMessages = 50;
        //                 }

        //                 if (maxMessages > 100)
        //                 {
        //                     maxMessages = 100;
        //                 }

        //                 List<StationMessage> messages = queueService.GetPendingMqttMessages(
        //                     maxMessages
        //                 );

        //                 return Results.Ok(messages.Select(ToApiMessage));
        //             }
        //             catch (TimeoutException)
        //             {
        //                 return Results.StatusCode(503);
        //             }
        //         }
        //     );
    }

    // =============================================================
    // API RESPONSE - EXCLUDE READING TIMESTAMP
    // =============================================================

    private static object ToApiMessage(StationMessage message)
    {
        return new
        {
            message.Id,
            message.Timestamp,
            message.StationName,
            message.MqttSent,
            message.FtpSent,
            message.Ftp2Sent,

            Measurements = message.Measurements.Select(reading => new
            {
                reading.StationName,
                reading.SlaveId,
                reading.Name,
                reading.Value,
                reading.Unit,
                reading.Status,
            }),
        };
    }
}
