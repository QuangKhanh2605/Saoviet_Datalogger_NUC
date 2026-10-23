using System.Globalization;
using System.Text.Json;
using Database.Configuration;
using Database.Models;
using Database.Services;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Database.Workers;

public sealed class DatabaseWorker : BackgroundService
{
    private static readonly TimeSpan SynchronizationInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DatabaseRetryInterval = TimeSpan.FromSeconds(5);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private const string RedisReadingPrefix = "modbus:readings:";

    private const string MqttStatusPrefix = "database:status:mqtt:";
    private const string FtpStatusPrefix = "database:status:ftp:";
    private const string Ftp2StatusPrefix = "database:status:ftp2:";

    private const int MaxRedisReadAttempts = 5;
    private const int RedisRetryDelayMilliseconds = 500;
    private const int RecoveryBatchSize = 50;

    private readonly ILogger<DatabaseWorker> _logger;
    private readonly DatabaseService _databaseService;
    private readonly MessageQueueService _queueService;
    private readonly IOptionsMonitor<AppSettings> _settings;
    private readonly IConnectionMultiplexer _redis;

    private readonly SemaphoreSlim _restartSignal = new(0, 1);

    private bool _databaseInitialized;
    private DateTime _nextDatabaseRetry = DateTime.MinValue;

    public DatabaseWorker(
        ILogger<DatabaseWorker> logger,
        DatabaseService databaseService,
        MessageQueueService queueService,
        IOptionsMonitor<AppSettings> settings,
        IConnectionMultiplexer redis
    )
    {
        _logger = logger;
        _databaseService = databaseService;
        _queueService = queueService;
        _settings = settings;
        _redis = redis;
    }

    // =========================================================
    // REQUEST RESTART
    // =========================================================

    public void RequestRestart()
    {
        if (_restartSignal.CurrentCount == 0)
        {
            _restartSignal.Release();
        }
    }

    // =========================================================
    // MAIN LOOP
    // =========================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DatabaseWorker started.");

        DateTime nextSaveTime = DateTime.MinValue;
        DateTime nextSyncTime = DateTime.UtcNow;
        DateTime nextCleanupTime = DateTime.UtcNow;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                TryInitializeDatabase();

                AppSettings settings = _settings.CurrentValue;

                if (!TryValidateSettings(settings, out string error))
                {
                    _logger.LogError("Invalid database settings: {Error}", error);

                    await Task.Delay(DatabaseRetryInterval, stoppingToken);
                    continue;
                }

                DateTime now = DateTime.UtcNow;

                if (nextSaveTime == DateTime.MinValue)
                {
                    nextSaveTime = CalculateNextSaveTime(settings, now);
                }

                // -------------------------------------------------
                // REDIS MODBUS SNAPSHOTS -> RAM QUEUE
                // -------------------------------------------------

                if (now >= nextSaveTime)
                {
                    try
                    {
                        await SaveModbusDataAsync(stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (RedisException ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Redis is unavailable while saving Modbus snapshots. "
                                + "The worker will retry."
                        );
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Failed to save Modbus snapshots. The worker will continue."
                        );
                    }

                    nextSaveTime = CalculateNextSaveTime(settings, DateTime.UtcNow);
                }

                now = DateTime.UtcNow;

                // -------------------------------------------------
                // PERIODIC SYNCHRONIZATION
                // -------------------------------------------------

                if (now >= nextSyncTime)
                {
                    await RunSyncTaskSafelyAsync(
                        SynchronizeArchiveAsync,
                        "Redis archive -> SQLite",
                        stoppingToken
                    );

                    await RunSyncTaskSafelyAsync(
                        SynchronizeSendStatusesAsync,
                        "Redis send statuses -> SQLite",
                        stoppingToken
                    );

                    await RunSyncTaskSafelyAsync(
                        RestorePendingMessagesAsync,
                        "SQLite pending messages -> Redis",
                        stoppingToken
                    );

                    nextSyncTime = DateTime.UtcNow + SynchronizationInterval;
                }

                now = DateTime.UtcNow;

                // -------------------------------------------------
                // SQLITE CLEANUP
                // -------------------------------------------------

                if (now >= nextCleanupTime)
                {
                    try
                    {
                        CleanupDatabase(settings, stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected database cleanup error.");
                    }

                    nextCleanupTime = DateTime.UtcNow.AddSeconds(
                        Math.Max(1, settings.Database.CleanupIntervalSeconds)
                    );
                }

                TimeSpan wait = CalculateWait(
                    DateTime.UtcNow,
                    nextSaveTime,
                    nextSyncTime,
                    nextCleanupTime
                );

                using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

                Task delayTask = Task.Delay(wait, waitCts.Token);
                Task restartTask = _restartSignal.WaitAsync(waitCts.Token);

                Task completed = await Task.WhenAny(delayTask, restartTask);

                await waitCts.CancelAsync();

                // Observe cancellation of the losing task.
                try
                {
                    await (completed == delayTask ? restartTask : delayTask);
                }
                catch (OperationCanceledException)
                {
                    // Expected when the other task completes first.
                }

                await completed;

                if (completed == restartTask && !stoppingToken.IsCancellationRequested)
                {
                    nextSaveTime = DateTime.MinValue;
                    nextSyncTime = DateTime.UtcNow;
                    nextCleanupTime = DateTime.UtcNow;

                    _logger.LogInformation("DatabaseWorker restart signal received.");
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DatabaseWorker encountered an unexpected error. Retrying.");

                try
                {
                    await Task.Delay(DatabaseRetryInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        _logger.LogInformation("DatabaseWorker stopped.");
    }

    // =========================================================
    // SAFE TASK EXECUTION
    // =========================================================

    private async Task RunSyncTaskSafelyAsync(
        Func<CancellationToken, Task> action,
        string taskName,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await action(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(
                ex,
                "{TaskName} failed because Redis is unavailable. "
                    + "Will retry during a later cycle.",
                taskName
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{TaskName} failed. Other worker tasks will continue.", taskName);
        }
    }

    // =========================================================
    // SQLITE INITIALIZATION
    // =========================================================

    private void TryInitializeDatabase()
    {
        if (_databaseInitialized || DateTime.UtcNow < _nextDatabaseRetry)
        {
            return;
        }

        try
        {
            _databaseService.Initialize();

            _databaseInitialized = true;
            _nextDatabaseRetry = DateTime.MinValue;

            _logger.LogInformation("SQLite initialization succeeded.");
        }
        catch (Exception ex)
        {
            _databaseInitialized = false;

            _nextDatabaseRetry = DateTime.UtcNow + DatabaseRetryInterval;

            _logger.LogWarning(
                ex,
                "SQLite is unavailable. Retrying initialization in {Delay} seconds.",
                DatabaseRetryInterval.TotalSeconds
            );
        }
    }

    // =========================================================
    // READ MODBUS SNAPSHOTS AND ENQUEUE MESSAGES
    // =========================================================

    private async Task SaveModbusDataAsync(CancellationToken cancellationToken)
    {
        List<SensorReading> readings = await ReadRedisReadingsWithRetryAsync(cancellationToken);

        if (readings.Count == 0)
        {
            _logger.LogDebug("No Modbus readings found in Redis.");

            return;
        }

        // Reload FTP configuration for every packaging cycle.
        var (ftp1Stations, ftp2Stations) = LoadFtpEnabledStations();

        List<StationMessage> messages = BuildStationMessages(readings, ftp1Stations, ftp2Stations);

        if (messages.Count == 0)
        {
            _logger.LogDebug("No station messages were created.");

            return;
        }

        int count = _queueService.EnqueueStationMessages(messages);

        _logger.LogInformation(
            "Created {Count} station messages from Redis snapshots. "
                + "FTP1 enabled stations: {Ftp1Count}; "
                + "FTP2 enabled stations: {Ftp2Count}.",
            count,
            ftp1Stations.Count,
            ftp2Stations.Count
        );
    }

    // =========================================================
    // READ FTP CONFIGURATION
    // =========================================================

    private (HashSet<string> Ftp1, HashSet<string> Ftp2) LoadFtpEnabledStations()
    {
        // Only use the current working directory.
        string configPath = Path.Combine(RuntimePaths.Root, "FtpConfig.json");

        if (!File.Exists(configPath))
        {
            _logger.LogWarning(
                "FtpConfig.json was not found at {ConfigPath}. "
                    + "FTP1 and FTP2 will be skipped for all stations.",
                configPath
            );

            return (
                new HashSet<string>(StringComparer.OrdinalIgnoreCase),
                new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            );
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(File.ReadAllText(configPath));

            JsonElement root = document.RootElement;

            HashSet<string> ftp1Stations = ReadEnabledStations(root, "Ftp");

            HashSet<string> ftp2Stations = ReadEnabledStations(root, "Ftp2");

            _logger.LogInformation(
                "Loaded FTP configuration from {ConfigPath}. "
                    + "FTP1 enabled stations: {Ftp1Count}; "
                    + "FTP2 enabled stations: {Ftp2Count}.",
                configPath,
                ftp1Stations.Count,
                ftp2Stations.Count
            );

            return (ftp1Stations, ftp2Stations);
        }
        catch (Exception ex)
            when (ex is IOException or UnauthorizedAccessException or JsonException)
        {
            _logger.LogError(
                ex,
                "Failed to load FTP configuration from {ConfigPath}. "
                    + "FTP1 and FTP2 will be skipped for all stations.",
                configPath
            );

            return (
                new HashSet<string>(StringComparer.OrdinalIgnoreCase),
                new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            );
        }
    }

    private static HashSet<string> ReadEnabledStations(JsonElement root, string sectionName)
    {
        HashSet<string> stations = new(StringComparer.OrdinalIgnoreCase);

        if (
            !TryGetPropertyIgnoreCase(root, sectionName, out JsonElement section)
            || section.ValueKind != JsonValueKind.Object
        )
        {
            return stations;
        }

        // The FTP destination itself must be enabled.
        if (
            !TryGetPropertyIgnoreCase(section, "Enabled", out JsonElement enabledElement)
            || enabledElement.ValueKind != JsonValueKind.True
        )
        {
            return stations;
        }

        if (
            !TryGetPropertyIgnoreCase(section, "Servers", out JsonElement servers)
            || servers.ValueKind != JsonValueKind.Array
        )
        {
            return stations;
        }

        foreach (JsonElement server in servers.EnumerateArray())
        {
            if (server.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (
                !TryGetPropertyIgnoreCase(server, "KYHIEU_TRAM", out JsonElement stationElement)
                || stationElement.ValueKind != JsonValueKind.String
            )
            {
                continue;
            }

            string? stationName = stationElement.GetString();

            if (!string.IsNullOrWhiteSpace(stationName))
            {
                stations.Add(stationName.Trim());
            }
        }

        return stations;
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement element,
        string propertyName,
        out JsonElement value
    )
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty property in element.EnumerateObject())
            {
                if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    // =========================================================
    // READ REDIS MODBUS READINGS
    // =========================================================

    private async Task<List<SensorReading>> ReadRedisReadingsWithRetryAsync(
        CancellationToken cancellationToken
    )
    {
        for (int attempt = 1; attempt <= MaxRedisReadAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await ReadRedisReadingsAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Redis read attempt {Attempt}/{MaxAttempts} failed.",
                    attempt,
                    MaxRedisReadAttempts
                );

                if (attempt < MaxRedisReadAttempts)
                {
                    await Task.Delay(RedisRetryDelayMilliseconds, cancellationToken);
                }
            }
        }

        _logger.LogError(
            "Unable to read Modbus snapshots from Redis after {Attempts} attempts. "
                + "The worker will retry in a later cycle.",
            MaxRedisReadAttempts
        );

        return new List<SensorReading>();
    }

    private async Task<List<SensorReading>> ReadRedisReadingsAsync(
        CancellationToken cancellationToken
    )
    {
        List<SensorReading> readings = new();

        IDatabase redisDatabase = _redis.GetDatabase();

        foreach (System.Net.EndPoint endpoint in _redis.GetEndPoints())
        {
            cancellationToken.ThrowIfCancellationRequested();

            IServer server = _redis.GetServer(endpoint);

            if (!server.IsConnected)
            {
                continue;
            }

            IEnumerable<RedisKey> keys = server.Keys(
                database: redisDatabase.Database,
                pattern: $"{RedisReadingPrefix}*",
                pageSize: 200
            );

            foreach (RedisKey key in keys)
            {
                cancellationToken.ThrowIfCancellationRequested();

                HashEntry[] fields = await redisDatabase.HashGetAllAsync(key);

                if (fields.Length == 0)
                {
                    continue;
                }

                string stationFromKey = key.ToString().Substring(RedisReadingPrefix.Length);

                foreach (HashEntry field in fields)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (field.Value.IsNullOrEmpty)
                    {
                        continue;
                    }

                    SensorReading? reading;

                    try
                    {
                        reading = JsonSerializer.Deserialize<SensorReading>(
                            field.Value.ToString(),
                            JsonOptions
                        );
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Invalid sensor JSON at {Key}, field {Field}.",
                            key,
                            field.Name
                        );

                        continue;
                    }

                    if (reading is null)
                    {
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(reading.StationName))
                    {
                        reading.StationName = stationFromKey;
                    }

                    if (string.IsNullOrWhiteSpace(reading.StationName))
                    {
                        continue;
                    }

                    readings.Add(reading);
                }
            }
        }

        return readings;
    }

    // =========================================================
    // BUILD STATION MESSAGES
    // =========================================================

    private List<StationMessage> BuildStationMessages(
        IEnumerable<SensorReading> readings,
        HashSet<string> ftp1Stations,
        HashSet<string> ftp2Stations
    )
    {
        string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);

        return readings
            .Where(reading => !string.IsNullOrWhiteSpace(reading.StationName))
            .GroupBy(reading => reading.StationName.Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                string stationName = group.Key;

                // A station is pending only if:
                // 1. The FTP destination is enabled.
                // 2. Its name exists in that destination's Servers list.
                bool ftp1Pending = ftp1Stations.Contains(stationName);

                bool ftp2Pending = ftp2Stations.Contains(stationName);

                _logger.LogInformation(
                    "Station {StationName}: "
                        + "FTP1={Ftp1Status} (FtpSent={FtpSent}); "
                        + "FTP2={Ftp2Status} (Ftp2Sent={Ftp2Sent}).",
                    stationName,
                    ftp1Pending ? "Pending" : "Skipped",
                    !ftp1Pending,
                    ftp2Pending ? "Pending" : "Skipped",
                    !ftp2Pending
                );

                return new StationMessage
                {
                    Id = 0,
                    Timestamp = timestamp,
                    StationName = stationName,

                    MqttSent = false,

                    FtpSent = !ftp1Pending,
                    Ftp2Sent = !ftp2Pending,

                    Measurements = group
                        .Select(reading => CloneReading(reading, timestamp))
                        .ToList(),
                };
            })
            .ToList();
    }

    private static SensorReading CloneReading(SensorReading reading, string messageTimestamp)
    {
        return new SensorReading
        {
            StationName = reading.StationName,
            SlaveId = reading.SlaveId,
            Name = reading.Name,
            Value = reading.Value,
            Unit = reading.Unit,
            Status = reading.Status,
            Timestamp = string.IsNullOrWhiteSpace(reading.Timestamp)
                ? messageTimestamp
                : reading.Timestamp,
        };
    }

    // =========================================================
    // REDIS ARCHIVE -> SQLITE
    // =========================================================

    private async Task SynchronizeArchiveAsync(CancellationToken cancellationToken)
    {
        if (!_databaseInitialized)
        {
            return;
        }

        try
        {
            await _queueService.SynchronizeAsync(cancellationToken);

            _logger.LogDebug("Redis archive synchronization completed.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(
                ex,
                "Redis archive synchronization failed. SQLite remains available; "
                    + "retrying next cycle."
            );
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Archive synchronization failed. It will be retried next cycle."
            );
        }
    }

    // =========================================================
    // REDIS SEND STATUS -> SQLITE
    // =========================================================

    private async Task SynchronizeSendStatusesAsync(CancellationToken cancellationToken)
    {
        if (!_databaseInitialized)
        {
            return;
        }

        try
        {
            await SynchronizeStatusPrefixAsync(MqttStatusPrefix, "mqtt", cancellationToken);

            await SynchronizeStatusPrefixAsync(FtpStatusPrefix, "ftp", cancellationToken);

            await SynchronizeStatusPrefixAsync(Ftp2StatusPrefix, "ftp2", cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(
                ex,
                "Redis is unavailable while synchronizing send statuses. "
                    + "Will retry next cycle."
            );
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to synchronize Redis send statuses to SQLite.");
        }
    }

    private async Task SynchronizeStatusPrefixAsync(
        string prefix,
        string destination,
        CancellationToken cancellationToken
    )
    {
        IDatabase redisDatabase = _redis.GetDatabase();

        foreach (System.Net.EndPoint endpoint in _redis.GetEndPoints())
        {
            cancellationToken.ThrowIfCancellationRequested();

            IServer server = _redis.GetServer(endpoint);

            if (!server.IsConnected)
            {
                continue;
            }

            foreach (
                RedisKey key in server.Keys(
                    database: redisDatabase.Database,
                    pattern: $"{prefix}*",
                    pageSize: 200
                )
            )
            {
                cancellationToken.ThrowIfCancellationRequested();

                string keyText = key.ToString();

                if (!keyText.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                string idText = keyText[prefix.Length..];

                if (
                    !long.TryParse(
                        idText,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out long messageId
                    )
                    || messageId <= 0
                )
                {
                    _logger.LogWarning("Ignoring invalid Redis status key {Key}.", keyText);

                    continue;
                }

                RedisValue status = await redisDatabase.StringGetAsync(key);

                if (status != "1")
                {
                    continue;
                }

                switch (destination)
                {
                    case "mqtt":
                        _databaseService.MarkMqttSent(new[] { messageId });
                        break;

                    case "ftp":
                        _databaseService.MarkFtpSent(new[] { messageId });
                        break;

                    case "ftp2":
                        _databaseService.MarkFtp2Sent(new[] { messageId });
                        break;
                }
            }
        }
    }

    // =========================================================
    // SQLITE -> REDIS STREAM RECOVERY
    // =========================================================

    private async Task RestorePendingMessagesAsync(CancellationToken cancellationToken)
    {
        if (!_databaseInitialized)
        {
            return;
        }

        try
        {
            int mqtt = await _queueService.RestorePendingMqttMessagesAsync(
                RecoveryBatchSize,
                cancellationToken
            );

            int ftp = await _queueService.RestorePendingFtpMessagesAsync(
                RecoveryBatchSize,
                cancellationToken
            );

            int ftp2 = await _queueService.RestorePendingFtp2MessagesAsync(
                RecoveryBatchSize,
                cancellationToken
            );

            if (mqtt > 0 || ftp > 0 || ftp2 > 0)
            {
                _logger.LogInformation(
                    "Restored pending messages: " + "MQTT={Mqtt}, FTP1={Ftp}, FTP2={Ftp2}.",
                    mqtt,
                    ftp,
                    ftp2
                );
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(
                ex,
                "Redis is unavailable during pending-message recovery. " + "Will retry next cycle."
            );
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to restore SQLite pending messages into Redis Streams.");
        }
    }

    // =========================================================
    // SQLITE CLEANUP
    // =========================================================

    private void CleanupDatabase(AppSettings settings, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_databaseInitialized)
        {
            return;
        }

        try
        {
            _databaseService.CleanupDatabase(
                settings.Database.RetentionDays,
                settings.Database.MaxDatabaseSizeMB,
                settings.Database.VacuumAfterCleanup
            );

            _logger.LogInformation("SQLite cleanup completed.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SQLite cleanup failed. Initialization will be retried.");

            _databaseInitialized = false;

            _nextDatabaseRetry = DateTime.UtcNow + DatabaseRetryInterval;
        }
    }

    // =========================================================
    // VALIDATE SETTINGS
    // =========================================================

    private static bool TryValidateSettings(AppSettings settings, out string error)
    {
        if (settings.Database is null)
        {
            error = "Database configuration is missing.";
            return false;
        }

        if (settings.Database.SaveIntervalMinutes <= 0)
        {
            error = "Database.SaveIntervalMinutes must be greater than zero.";
            return false;
        }

        if (settings.Database.SaveSecond is < 0 or > 59)
        {
            error = "Database.SaveSecond must be between 0 and 59.";
            return false;
        }

        if (settings.Database.RetentionDays <= 0)
        {
            error = "Database.RetentionDays must be greater than zero.";
            return false;
        }

        if (settings.Database.MaxDatabaseSizeMB <= 0)
        {
            error = "Database.MaxDatabaseSizeMB must be greater than zero.";
            return false;
        }

        if (settings.Database.CleanupIntervalSeconds <= 0)
        {
            error = "Database.CleanupIntervalSeconds must be greater than zero.";
            return false;
        }

        error = "";
        return true;
    }

    // =========================================================
    // SCHEDULE
    // =========================================================

    private static DateTime CalculateNextSaveTime(AppSettings settings, DateTime now)
    {
        DateTime candidate = new(
            now.Year,
            now.Month,
            now.Day,
            now.Hour,
            now.Minute,
            settings.Database.SaveSecond,
            DateTimeKind.Utc
        );

        if (candidate <= now)
        {
            candidate = candidate.AddMinutes(settings.Database.SaveIntervalMinutes);
        }

        return candidate;
    }

    private static TimeSpan CalculateWait(
        DateTime now,
        DateTime nextSaveTime,
        DateTime nextSyncTime,
        DateTime nextCleanupTime
    )
    {
        DateTime next = new[] { nextSaveTime, nextSyncTime, nextCleanupTime }.Min();

        TimeSpan wait = next - now;

        return wait < TimeSpan.FromMilliseconds(100) ? TimeSpan.FromMilliseconds(100) : wait;
    }

    // =========================================================
    // DISPOSE
    // =========================================================

    public override void Dispose()
    {
        _restartSignal.Dispose();
        base.Dispose();
    }
}
