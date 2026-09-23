using Database.Models;
using Database.Services;

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
        // MQTT - GET PENDING
        // =============================================================

        app.MapGet(
            "/api/mqtt/pending",
            (MessageQueueService queueService, int? maxMessages) =>
            {
                try
                {
                    int max = maxMessages.GetValueOrDefault(10);

                    if (max <= 0)
                    {
                        max = 10;
                    }

                    if (max > 100)
                    {
                        max = 100;
                    }

                    List<StationMessage> messages = queueService.GetPendingMqttMessages(max);

                    return Results.Ok(messages);
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );

        // =============================================================
        // MQTT - MARK SENT
        // =============================================================

        app.MapPost(
            "/api/mqtt/mark-sent",
            (MessageQueueService queueService, MarkMessagesSentRequest request) =>
            {
                if (request.Ids == null || request.Ids.Count == 0)
                {
                    return Results.BadRequest(new { error = "Ids cannot be empty." });
                }

                try
                {
                    int markedCount = queueService.MarkMqttSent(request.Ids);

                    return Results.Ok(
                        new
                        {
                            success = true,

                            type = "mqtt",

                            markedCount,

                            ids = request.Ids,
                        }
                    );
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );

        // =============================================================
        // FTP - GET PENDING
        // =============================================================

        app.MapGet(
            "/api/ftp/pending",
            (MessageQueueService queueService, string? stationNames, int? maxMessages) =>
            {
                try
                {
                    int max = maxMessages.GetValueOrDefault(10);

                    if (max <= 0)
                    {
                        max = 10;
                    }

                    if (max > 100)
                    {
                        max = 100;
                    }

                    List<string>? stations = null;

                    if (!string.IsNullOrWhiteSpace(stationNames))
                    {
                        stations = stationNames
                            .Split(
                                ',',
                                StringSplitOptions.RemoveEmptyEntries
                                    | StringSplitOptions.TrimEntries
                            )
                            .ToList();
                    }

                    List<StationMessage> messages = queueService.GetPendingFtpMessages(
                        stations,
                        max
                    );

                    return Results.Ok(messages);
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );

        // =============================================================
        // FTP - MARK SENT
        // =============================================================

        app.MapPost(
            "/api/ftp/mark-sent",
            (MessageQueueService queueService, MarkMessagesSentRequest request) =>
            {
                if (request.Ids == null || request.Ids.Count == 0)
                {
                    return Results.BadRequest(new { error = "Ids cannot be empty." });
                }

                try
                {
                    int markedCount = queueService.MarkFtpSent(request.Ids);

                    return Results.Ok(
                        new
                        {
                            success = true,

                            type = "ftp",

                            markedCount,

                            ids = request.Ids,
                        }
                    );
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );

        // =============================================================
        // FTP2 - GET PENDING
        // =============================================================

        app.MapGet(
            "/api/ftp2/pending",
            (MessageQueueService queueService, string? stationNames, int? maxMessages) =>
            {
                try
                {
                    int max = maxMessages.GetValueOrDefault(10);

                    if (max <= 0)
                    {
                        max = 10;
                    }

                    if (max > 100)
                    {
                        max = 100;
                    }

                    List<string>? stations = null;

                    if (!string.IsNullOrWhiteSpace(stationNames))
                    {
                        stations = stationNames
                            .Split(
                                ',',
                                StringSplitOptions.RemoveEmptyEntries
                                    | StringSplitOptions.TrimEntries
                            )
                            .ToList();
                    }

                    List<StationMessage> messages = queueService.GetPendingFtp2Messages(
                        stations,
                        max
                    );

                    return Results.Ok(messages);
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );

        // =============================================================
        // FTP2 - MARK SENT
        // =============================================================

        app.MapPost(
            "/api/ftp2/mark-sent",
            (MessageQueueService queueService, MarkMessagesSentRequest request) =>
            {
                if (request.Ids == null || request.Ids.Count == 0)
                {
                    return Results.BadRequest(new { error = "Ids cannot be empty." });
                }

                try
                {
                    int markedCount = queueService.MarkFtp2Sent(request.Ids);

                    return Results.Ok(
                        new
                        {
                            success = true,

                            type = "ftp2",

                            markedCount,

                            ids = request.Ids,
                        }
                    );
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );

        // =============================================================
        // DEBUG - MQTT QUEUE
        // =============================================================

        app.MapGet(
            "/api/messages",
            (MessageQueueService queueService, int? max) =>
            {
                try
                {
                    int maxMessages = max.GetValueOrDefault(50);

                    if (maxMessages <= 0)
                    {
                        maxMessages = 50;
                    }

                    if (maxMessages > 100)
                    {
                        maxMessages = 100;
                    }

                    List<StationMessage> messages = queueService.GetPendingMqttMessages(
                        maxMessages
                    );

                    return Results.Ok(messages);
                }
                catch (TimeoutException)
                {
                    return Results.StatusCode(503);
                }
            }
        );
    }
}
