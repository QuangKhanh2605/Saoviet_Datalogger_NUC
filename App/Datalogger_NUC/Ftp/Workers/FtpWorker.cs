using System.Text;
using System.Text.Json;
using Ftp.Configuration;
using Ftp.Models;
using Ftp.Services;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Ftp.Workers;

public sealed class FtpWorker : BackgroundService
{
    private const int MaxMessagesPerFile = 50;
    private const int RedisReadBatchSize = 1000;
    private const int LoopDelaySeconds = 1;
    private const int ConfigurationRetryDelaySeconds = 5;

    private const string Ftp1Stream = "database:stream:ftp";
    private const string Ftp2Stream = "database:stream:ftp2";

    private const string Ftp1StatusPrefix = "database:status:ftp:";
    private const string Ftp2StatusPrefix = "database:status:ftp2:";

    private const string Ftp1DedupPrefix = "database:dedup:ftp:";
    private const string Ftp2DedupPrefix = "database:dedup:ftp2:";

    private const string PayloadField = "payload";

    private const string MarkSentScript = """
        local stream = KEYS[1]
        local statusPrefix = ARGV[1]
        local dedupPrefix = ARGV[2]
        local processed = 0

        for i = 3, #ARGV, 2 do
            local messageId = ARGV[i]
            local entryId = ARGV[i + 1]

            redis.call('SET', statusPrefix .. messageId, '1')
            redis.call('XDEL', stream, entryId)
            redis.call('DEL', dedupPrefix .. messageId)

            processed = processed + 1
        end

        return processed
        """;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly ILogger<FtpWorker> _logger;
    private readonly FtpService _ftpService;
    private readonly IOptionsMonitor<AppSettings> _options;
    private readonly IDatabase _redis;

    private readonly SemaphoreSlim _restartSignal = new(0, 1);

    // Lưu ánh xạ MessageId -> Redis Stream EntryId để xóa đúng entry
    // sau khi FTP upload thành công.
    private readonly Dictionary<
        (FtpQueueType QueueType, long MessageId),
        RedisValue
    > _streamEntryIds = new();

    private volatile bool _workerRunning;

    private DateTime? _lastSuccessfulProcess;
    private DateTime? _lastErrorTime;
    private string? _lastError;

    public FtpWorker(
        ILogger<FtpWorker> logger,
        FtpService ftpService,
        IOptionsMonitor<AppSettings> options,
        IConnectionMultiplexer redis
    )
    {
        _logger = logger;
        _ftpService = ftpService;
        _options = options;
        _redis = redis.GetDatabase();
    }

    private sealed record FtpTarget(string Name, FtpSettings Settings, FtpQueueType QueueType);

    private enum FtpQueueType
    {
        Ftp1,
        Ftp2,
    }

    // ================================================================
    // REQUEST RESTART
    // ================================================================

    public void RequestRestart()
    {
        try
        {
            _restartSignal.Release();
            _logger.LogInformation("FTP session restart requested.");
        }
        catch (SemaphoreFullException)
        {
            // Đã có yêu cầu restart đang chờ xử lý.
        }
        catch (ObjectDisposedException)
        {
            _logger.LogDebug("FTP restart signal đã được giải phóng.");
        }
    }

    // ================================================================
    // WAIT FOR RESTART REQUEST
    // ================================================================

    private async Task WaitForRestartAsync(CancellationToken cancellationToken)
    {
        await _restartSignal.WaitAsync(cancellationToken);

        while (_restartSignal.Wait(0))
        {
            // Gộp nhiều yêu cầu restart thành một lần.
        }
    }

    // ================================================================
    // CREATE FTP TARGETS
    // ================================================================

    private List<FtpTarget> CreateFtpTargets()
    {
        AppSettings settings = _options.CurrentValue;

        return new List<FtpTarget>
        {
            new("FTP1", settings.Ftp, FtpQueueType.Ftp1),
            new("FTP2", settings.Ftp2, FtpQueueType.Ftp2),
        };
    }

    // ================================================================
    // WORKER LIFECYCLE
    // ================================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("FTP Worker started.");
        _workerRunning = true;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(
                    stoppingToken
                );

                using var restartCts = CancellationTokenSource.CreateLinkedTokenSource(
                    stoppingToken
                );

                Task sessionTask = RunSessionAsync(sessionCts.Token);
                Task restartTask = WaitForRestartAsync(restartCts.Token);

                Task completed = await Task.WhenAny(sessionTask, restartTask);

                await restartCts.CancelAsync();

                try
                {
                    await restartTask;
                }
                catch (OperationCanceledException) when (restartCts.IsCancellationRequested)
                {
                    // Bình thường khi phiên kết thúc hoặc ứng dụng shutdown.
                }

                if (stoppingToken.IsCancellationRequested)
                {
                    await sessionCts.CancelAsync();
                }
                else if (completed == restartTask)
                {
                    _logger.LogInformation("Restarting FTP session to reload configuration.");

                    await sessionCts.CancelAsync();
                }
                else
                {
                    await sessionCts.CancelAsync();
                }

                try
                {
                    await sessionTask;
                }
                catch (OperationCanceledException) when (sessionCts.IsCancellationRequested)
                {
                    // Bình thường khi restart hoặc shutdown.
                }
                catch (Exception ex)
                {
                    SaveError(ex);

                    _logger.LogError(ex, "FTP session ended unexpectedly. Worker will retry.");
                }

                if (stoppingToken.IsCancellationRequested)
                    break;

                if (completed == sessionTask)
                {
                    try
                    {
                        await Task.Delay(
                            TimeSpan.FromSeconds(ConfigurationRetryDelaySeconds),
                            stoppingToken
                        );
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                }

                _logger.LogInformation("Starting FTP session with current configuration.");
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("FTP Worker stopping.");
        }
        catch (Exception ex)
        {
            SaveError(ex);

            _logger.LogCritical(ex, "FTP Worker encountered an unexpected fatal error.");
        }
        finally
        {
            _workerRunning = false;
            _logger.LogInformation("FTP Worker stopped.");
        }
    }

    // ================================================================
    // RUN ONE FTP SESSION
    // ================================================================

    private async Task RunSessionAsync(CancellationToken sessionToken)
    {
        List<FtpTarget> ftpTargets = CreateFtpTargets();

        ValidateFtpTargets(ftpTargets);

        _logger.LogInformation(
            "FTP session started. FTP1 enabled={Ftp1Enabled}, FTP2 enabled={Ftp2Enabled}.",
            ftpTargets[0].Settings.Enabled,
            ftpTargets[1].Settings.Enabled
        );

        while (!sessionToken.IsCancellationRequested)
        {
            // Giữ thứ tự FTP1 -> FTP2.
            foreach (FtpTarget target in ftpTargets)
            {
                sessionToken.ThrowIfCancellationRequested();

                try
                {
                    await ProcessServersAsync(target, sessionToken);
                }
                catch (OperationCanceledException) when (sessionToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    SaveError(ex);

                    _logger.LogError(
                        ex,
                        "{FtpType} processing failed unexpectedly. Will retry next loop.",
                        target.Name
                    );
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(LoopDelaySeconds), sessionToken);
        }
    }

    // ================================================================
    // PROCESS SERVERS
    // ================================================================

    private async Task ProcessServersAsync(FtpTarget target, CancellationToken stoppingToken)
    {
        FtpSettings settings = target.Settings;

        if (!settings.Enabled || settings.Servers.Count == 0)
            return;

        foreach (FtpServerSettings server in settings.Servers)
        {
            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                await ProcessServerAsync(target, server, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                SaveError(ex);

                _logger.LogError(
                    ex,
                    "{FtpType} error. Station={Station}. Retry next loop.",
                    target.Name,
                    server.KYHIEU_TRAM
                );
            }
        }
    }

    // ================================================================
    // PROCESS SERVER
    // ================================================================

    private async Task ProcessServerAsync(
        FtpTarget target,
        FtpServerSettings server,
        CancellationToken stoppingToken
    )
    {
        switch (server.PackageMode)
        {
            case 1:
                await ProcessMode1Async(target, server, stoppingToken);
                break;

            case 2:
                await ProcessMode2Async(target, server, stoppingToken);
                break;

            case 3:
                await ProcessMode3Async(target, server, stoppingToken);
                break;

            default:
                throw new ArgumentOutOfRangeException(
                    nameof(server.PackageMode),
                    server.PackageMode,
                    "PackageMode phải là 1, 2 hoặc 3."
                );
        }
    }

    // ================================================================
    // MODE 1
    // ================================================================

    private async Task ProcessMode1Async(
        FtpTarget target,
        FtpServerSettings server,
        CancellationToken stoppingToken
    )
    {
        List<StationMessage> messages = await GetPendingFtpMessagesAsync(
            target,
            server.KYHIEU_TRAM,
            stoppingToken
        );

        if (messages.Count == 0)
            return;

        foreach (StationMessage message in messages)
        {
            stoppingToken.ThrowIfCancellationRequested();

            foreach (SensorReading reading in message.Measurements)
            {
                stoppingToken.ThrowIfCancellationRequested();

                DateTime packageTimestamp = DateTime.Now;

                string fileName = BuildMode1FileName(server, reading.Name, packageTimestamp);

                string content = BuildMode1Content(message, reading);

                bool success = await _ftpService.UploadBytesAsync(
                    server,
                    fileName,
                    Encoding.UTF8.GetBytes(content),
                    packageTimestamp,
                    stoppingToken
                );

                if (!success)
                {
                    _logger.LogWarning(
                        "{FtpType} Mode 1 upload failed. MessageId={MessageId}, Station={Station}, Parameter={Parameter}. Retry next loop.",
                        target.Name,
                        message.Id,
                        message.StationName,
                        reading.Name
                    );

                    return;
                }
            }

            await MarkFtpSentAsync(target, new[] { message.Id }, stoppingToken);

            _lastSuccessfulProcess = DateTime.Now;

            _logger.LogInformation(
                "{FtpType} Mode 1 OK. MessageId={MessageId}, Station={Station}",
                target.Name,
                message.Id,
                message.StationName
            );
        }
    }

    // ================================================================
    // MODE 2
    // ================================================================

    private async Task ProcessMode2Async(
        FtpTarget target,
        FtpServerSettings server,
        CancellationToken stoppingToken
    )
    {
        List<StationMessage> messages = await GetPendingFtpMessagesAsync(
            target,
            server.KYHIEU_TRAM,
            stoppingToken
        );

        if (messages.Count == 0)
            return;

        DateTime packageTimestamp = DateTime.Now;

        string fileName = BuildMode2FileName(server, packageTimestamp);
        string content = BuildMode2Content(messages);

        bool success = await _ftpService.UploadBytesAsync(
            server,
            fileName,
            Encoding.UTF8.GetBytes(content),
            packageTimestamp,
            stoppingToken
        );

        if (!success)
        {
            _logger.LogWarning(
                "{FtpType} Mode 2 upload failed. Station={Station}, Messages={Count}. Retry next loop.",
                target.Name,
                server.KYHIEU_TRAM,
                messages.Count
            );

            return;
        }

        await MarkFtpSentAsync(target, messages.Select(x => x.Id), stoppingToken);

        _lastSuccessfulProcess = DateTime.Now;

        _logger.LogInformation(
            "{FtpType} Mode 2 OK. Station={Station}, Messages={Count}",
            target.Name,
            server.KYHIEU_TRAM,
            messages.Count
        );
    }

    // ================================================================
    // MODE 3
    // ================================================================

    private async Task ProcessMode3Async(
        FtpTarget target,
        FtpServerSettings server,
        CancellationToken stoppingToken
    )
    {
        FtpSettings settings = target.Settings;

        List<string> stationNames = settings
            .Servers.Where(x =>
                string.Equals(x.MA_TINH, server.MA_TINH, StringComparison.OrdinalIgnoreCase)
                && string.Equals(
                    x.KYHIEU_CONGTRINH,
                    server.KYHIEU_CONGTRINH,
                    StringComparison.OrdinalIgnoreCase
                )
            )
            .Select(x => x.KYHIEU_TRAM)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (stationNames.Count == 0)
            return;

        List<StationMessage> messages = await GetPendingFtpMessagesAsync(
            target,
            stationNames,
            stoppingToken
        );

        if (messages.Count == 0)
            return;

        DateTime packageTimestamp = DateTime.Now;

        string fileName = BuildMode3FileName(server, packageTimestamp);
        string content = BuildMode3Content(messages);

        bool success = await _ftpService.UploadBytesAsync(
            server,
            fileName,
            Encoding.UTF8.GetBytes(content),
            packageTimestamp,
            stoppingToken
        );

        if (!success)
        {
            _logger.LogWarning(
                "{FtpType} Mode 3 upload failed. CongTrinh={CongTrinh}, Messages={Count}. Retry next loop.",
                target.Name,
                server.KYHIEU_CONGTRINH,
                messages.Count
            );

            return;
        }

        await MarkFtpSentAsync(target, messages.Select(x => x.Id), stoppingToken);

        _lastSuccessfulProcess = DateTime.Now;

        _logger.LogInformation(
            "{FtpType} Mode 3 OK. CongTrinh={CongTrinh}, Messages={Count}",
            target.Name,
            server.KYHIEU_CONGTRINH,
            messages.Count
        );
    }

    // ================================================================
    // REDIS - GET PENDING MESSAGES
    // ================================================================

    private Task<List<StationMessage>> GetPendingFtpMessagesAsync(
        FtpTarget target,
        string stationName,
        CancellationToken stoppingToken
    )
    {
        return GetPendingFtpMessagesAsync(target, new[] { stationName }, stoppingToken);
    }

    private async Task<List<StationMessage>> GetPendingFtpMessagesAsync(
        FtpTarget target,
        IEnumerable<string> stationNames,
        CancellationToken stoppingToken
    )
    {
        stoppingToken.ThrowIfCancellationRequested();

        RedisKey stream = GetStreamKey(target.QueueType);

        HashSet<string> stationFilter = stationNames
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (stationFilter.Count == 0)
            return new List<StationMessage>();

        StreamEntry[] entries = await _redis.StreamRangeAsync(
            stream,
            minId: "-",
            maxId: "+",
            count: RedisReadBatchSize,
            messageOrder: Order.Ascending
        );

        var messages = new List<StationMessage>(MaxMessagesPerFile);
        var seenMessageIds = new HashSet<long>();

        foreach (StreamEntry entry in entries)
        {
            stoppingToken.ThrowIfCancellationRequested();

            RedisValue payload = RedisValue.Null;

            foreach (NameValueEntry field in entry.Values)
            {
                if (field.Name == PayloadField)
                {
                    payload = field.Value;
                    break;
                }
            }

            if (payload.IsNullOrEmpty)
            {
                _logger.LogWarning(
                    "Redis entry has no payload. Stream={Stream}, EntryId={EntryId}. Entry retained.",
                    stream,
                    entry.Id
                );

                continue;
            }

            StationMessage? message;

            try
            {
                message = JsonSerializer.Deserialize<StationMessage>(
                    payload.ToString(),
                    JsonOptions
                );
            }
            catch (JsonException ex)
            {
                _logger.LogError(
                    ex,
                    "Cannot deserialize Redis payload. Stream={Stream}, EntryId={EntryId}. Entry retained.",
                    stream,
                    entry.Id
                );

                continue;
            }

            if (message is null || message.Id <= 0)
            {
                _logger.LogWarning(
                    "Invalid station message in Redis. Stream={Stream}, EntryId={EntryId}. Entry retained.",
                    stream,
                    entry.Id
                );

                continue;
            }

            if (
                string.IsNullOrWhiteSpace(message.StationName)
                || !stationFilter.Contains(message.StationName)
            )
            {
                continue;
            }

            if (!seenMessageIds.Add(message.Id))
                continue;

            _streamEntryIds[(target.QueueType, message.Id)] = entry.Id;

            messages.Add(message);

            if (messages.Count >= MaxMessagesPerFile)
                break;
        }

        return messages;
    }

    // ================================================================
    // REDIS - MARK SENT
    // ================================================================

    private async Task MarkFtpSentAsync(
        FtpTarget target,
        IEnumerable<long> messageIds,
        CancellationToken stoppingToken
    )
    {
        stoppingToken.ThrowIfCancellationRequested();

        RedisKey stream = GetStreamKey(target.QueueType);
        string statusPrefix = GetStatusPrefix(target.QueueType);
        string dedupPrefix = GetDedupPrefix(target.QueueType);

        long[] distinctIds = messageIds.Where(id => id > 0).Distinct().ToArray();

        if (distinctIds.Length == 0)
            return;

        var references = new List<(long MessageId, RedisValue EntryId)>();

        foreach (long messageId in distinctIds)
        {
            if (_streamEntryIds.TryGetValue((target.QueueType, messageId), out RedisValue entryId))
            {
                references.Add((messageId, entryId));
            }
            else
            {
                // Không đánh dấu thành công nếu không xác định được entry.
                // Giữ nguyên dữ liệu để tránh xóa nhầm bản tin.
                throw new InvalidOperationException(
                    $"Không tìm thấy Redis Stream EntryId cho "
                        + $"{target.Name}, MessageId={messageId}. "
                        + "Không thể xác nhận bản tin đã gửi."
                );
            }
        }

        var arguments = new RedisValue[2 + references.Count * 2];

        arguments[0] = statusPrefix;
        arguments[1] = dedupPrefix;

        int argumentIndex = 2;

        foreach (var reference in references)
        {
            arguments[argumentIndex++] = reference.MessageId.ToString();
            arguments[argumentIndex++] = reference.EntryId;
        }

        RedisResult result = await _redis.ScriptEvaluateAsync(
            MarkSentScript,
            new RedisKey[] { stream },
            arguments
        );

        long processed = (long)result;

        if (processed != references.Count)
        {
            throw new InvalidOperationException(
                $"Redis mark-sent không xử lý đủ bản tin. "
                    + $"Expected={references.Count}, Actual={processed}."
            );
        }

        foreach (var reference in references)
        {
            _streamEntryIds.Remove((target.QueueType, reference.MessageId));
        }

        _logger.LogInformation(
            "{FtpType} marked {Count} messages as sent in Redis.",
            target.Name,
            processed
        );
    }

    // ================================================================
    // REDIS - QUEUE KEYS
    // ================================================================

    private static RedisKey GetStreamKey(FtpQueueType queueType)
    {
        return queueType switch
        {
            FtpQueueType.Ftp1 => Ftp1Stream,
            FtpQueueType.Ftp2 => Ftp2Stream,
            _ => throw new ArgumentOutOfRangeException(nameof(queueType)),
        };
    }

    private static string GetStatusPrefix(FtpQueueType queueType)
    {
        return queueType switch
        {
            FtpQueueType.Ftp1 => Ftp1StatusPrefix,
            FtpQueueType.Ftp2 => Ftp2StatusPrefix,
            _ => throw new ArgumentOutOfRangeException(nameof(queueType)),
        };
    }

    private static string GetDedupPrefix(FtpQueueType queueType)
    {
        return queueType switch
        {
            FtpQueueType.Ftp1 => Ftp1DedupPrefix,
            FtpQueueType.Ftp2 => Ftp2DedupPrefix,
            _ => throw new ArgumentOutOfRangeException(nameof(queueType)),
        };
    }

    // ================================================================
    // FILE NAME
    // ================================================================

    private static string BuildMode1FileName(
        FtpServerSettings server,
        string name,
        DateTime timestamp
    )
    {
        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{server.KYHIEU_TRAM}_"
            + $"{name}_"
            + $"{timestamp:yyyyMMddHHmmss}.txt";
    }

    private static string BuildMode2FileName(FtpServerSettings server, DateTime timestamp)
    {
        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{server.KYHIEU_TRAM}_"
            + $"{timestamp:yyyyMMddHHmmss}.txt";
    }

    private static string BuildMode3FileName(FtpServerSettings server, DateTime timestamp)
    {
        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{timestamp:yyyyMMddHHmmss}.txt";
    }

    // ================================================================
    // CONTENT
    // ================================================================

    private static string BuildMode1Content(StationMessage message, SensorReading reading)
    {
        return $"{message.Timestamp}\t"
            + $"{reading.Value}\t"
            + $"{reading.Unit}\t"
            + $"{reading.Status:D2}"
            + Environment.NewLine;
    }

    private static string BuildMode2Content(IEnumerable<StationMessage> messages)
    {
        var builder = new StringBuilder();

        foreach (StationMessage message in messages)
        {
            foreach (SensorReading reading in message.Measurements)
            {
                builder.AppendLine(
                    $"{reading.Name}\t"
                        + $"{reading.Value}\t"
                        + $"{reading.Unit}\t"
                        + $"{message.Timestamp}\t"
                        + $"{reading.Status:D2}"
                );
            }
        }

        return builder.ToString();
    }

    private static string BuildMode3Content(IEnumerable<StationMessage> messages)
    {
        var builder = new StringBuilder();

        foreach (StationMessage message in messages)
        {
            foreach (SensorReading reading in message.Measurements)
            {
                builder.AppendLine(
                    $"{message.StationName}\t"
                        + $"{reading.Name}\t"
                        + $"{reading.Value}\t"
                        + $"{reading.Unit}\t"
                        + $"{message.Timestamp}\t"
                        + $"{reading.Status:D2}"
                );
            }
        }

        return builder.ToString();
    }

    // ================================================================
    // CONFIGURATION VALIDATION
    // ================================================================

    private static void ValidateFtpTargets(List<FtpTarget> ftpTargets)
    {
        foreach (FtpTarget target in ftpTargets)
        {
            if (target.Settings is null)
            {
                throw new ArgumentException($"{target.Name} configuration không được null.");
            }

            ValidateFtpSettings(target.Settings, target.Name);
        }
    }

    private static void ValidateFtpSettings(FtpSettings settings, string ftpType)
    {
        if (!settings.Enabled)
            return;

        if (settings.Servers is null || settings.Servers.Count == 0)
        {
            throw new ArgumentException($"{ftpType} enabled nhưng không có server.");
        }

        foreach (FtpServerSettings server in settings.Servers)
        {
            if (string.IsNullOrWhiteSpace(server.MA_TINH))
            {
                throw new ArgumentException($"{ftpType} MA_TINH không được rỗng.");
            }

            if (string.IsNullOrWhiteSpace(server.KYHIEU_CONGTRINH))
            {
                throw new ArgumentException($"{ftpType} KYHIEU_CONGTRINH không được rỗng.");
            }

            if (string.IsNullOrWhiteSpace(server.KYHIEU_TRAM))
            {
                throw new ArgumentException($"{ftpType} KYHIEU_TRAM không được rỗng.");
            }

            if (string.IsNullOrWhiteSpace(server.IP))
            {
                throw new ArgumentException($"{ftpType} IP không được rỗng.");
            }

            if (server.Port <= 0 || server.Port > 65535)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(server.Port),
                    server.Port,
                    $"{ftpType} Port không hợp lệ."
                );
            }

            if (server.PackageMode < 1 || server.PackageMode > 3)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(server.PackageMode),
                    server.PackageMode,
                    $"{ftpType} PackageMode phải là 1, 2 hoặc 3."
                );
            }

            if (server.ModePath < 0 || server.ModePath > 2)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(server.ModePath),
                    server.ModePath,
                    $"{ftpType} ModePath phải là 0, 1 hoặc 2."
                );
            }
        }
    }

    // ================================================================
    // STATUS
    // ================================================================

    private void SaveError(Exception ex)
    {
        _lastErrorTime = DateTime.Now;
        _lastError = ex.Message;
    }

    public object GetHealthStatus()
    {
        return new
        {
            status = _workerRunning ? "Healthy" : "Unhealthy",
            service = "FTP",
            worker = _workerRunning ? "Running" : "Stopped",
        };
    }

    public object GetStatus()
    {
        List<FtpTarget> ftpTargets = CreateFtpTargets();

        return new
        {
            service = "FTP",
            worker = _workerRunning ? "Running" : "Stopped",

            ftp = ftpTargets.Select(x => new
            {
                name = x.Name,
                enabled = x.Settings.Enabled,
                servers = x.Settings.Servers?.Count ?? 0,
            }),

            lastSuccessfulProcess = _lastSuccessfulProcess,
            lastErrorTime = _lastErrorTime,
            lastError = _lastError,
        };
    }

    // ================================================================
    // DISPOSE
    // ================================================================

    public override void Dispose()
    {
        _restartSignal.Dispose();
        base.Dispose();
    }
}
