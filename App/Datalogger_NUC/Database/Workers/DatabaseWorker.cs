using System.Net.Http.Json;
using Database.Configuration;
using Database.Models;
using Database.Services;
using Microsoft.Extensions.Options;

namespace Database.Workers;

public sealed class DatabaseWorker : BackgroundService
{
    private static readonly TimeSpan SynchronizationInterval = TimeSpan.FromSeconds(5);

    private readonly ILogger<DatabaseWorker> _logger;
    private readonly DatabaseService _databaseService;
    private readonly MessageQueueService _queueService;
    private readonly AppSettings _settings;
    private readonly IHttpClientFactory _httpClientFactory;

    public DatabaseWorker(
        ILogger<DatabaseWorker> logger,
        DatabaseService databaseService,
        MessageQueueService queueService,
        IOptions<AppSettings> settings,
        IHttpClientFactory httpClientFactory
    )
    {
        _logger = logger;
        _databaseService = databaseService;
        _queueService = queueService;
        _settings = settings.Value;
        _httpClientFactory = httpClientFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DatabaseWorker started.");

        ValidateSettings();

        DateTime now = DateTime.Now;

        DateTime nextSaveTime = CalculateNextSaveTime(now);

        DateTime nextSynchronizationTime = now + SynchronizationInterval;

        DateTime nextCleanupTime = now.AddSeconds(_settings.Database.CleanupIntervalSeconds);

        _logger.LogInformation("Next data collection scheduled at {NextSaveTime}.", nextSaveTime);

        _logger.LogInformation(
            "Next queue synchronization scheduled at {NextSynchronizationTime}.",
            nextSynchronizationTime
        );

        _logger.LogInformation(
            "Next database cleanup scheduled at {NextCleanupTime}.",
            nextCleanupTime
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                now = DateTime.Now;

                // =================================================
                // READ MODBUS → 3 RAM QUEUES
                // =================================================

                if (now >= nextSaveTime)
                {
                    await SaveModbusDataAsync(stoppingToken);

                    nextSaveTime = CalculateNextSaveTime(DateTime.Now);
                }

                // =================================================
                // RAM ↔ SQLITE
                // =================================================

                if (now >= nextSynchronizationTime)
                {
                    await SynchronizeQueuesAsync(stoppingToken);

                    nextSynchronizationTime = DateTime.Now + SynchronizationInterval;
                }

                // =================================================
                // DATABASE CLEANUP
                // =================================================

                if (now >= nextCleanupTime)
                {
                    CleanupDatabase();

                    nextCleanupTime = DateTime.Now.AddSeconds(
                        _settings.Database.CleanupIntervalSeconds
                    );
                }

                // =================================================
                // CALCULATE NEXT EVENT
                // =================================================

                DateTime nextEvent = nextSaveTime;

                if (nextSynchronizationTime < nextEvent)
                {
                    nextEvent = nextSynchronizationTime;
                }

                if (nextCleanupTime < nextEvent)
                {
                    nextEvent = nextCleanupTime;
                }

                TimeSpan delay = nextEvent - DateTime.Now;

                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in DatabaseWorker.");

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        _logger.LogInformation("DatabaseWorker stopped.");
    }

    // =============================================================
    // SYNCHRONIZE RAM ↔ SQLITE
    // =============================================================

    private async Task SynchronizeQueuesAsync(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug("Starting RAM queue synchronization.");

            await _queueService.SynchronizeAsync(cancellationToken);

            _logger.LogDebug("RAM queue synchronization completed.");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to synchronize RAM queues with SQLite.");
        }
    }

    // =============================================================
    // READ MODBUS → RAM
    // =============================================================

    private async Task SaveModbusDataAsync(CancellationToken cancellationToken)
    {
        const int MaxAttempts = 5;
        const int RetryDelayMilliseconds = 500;

        try
        {
            _logger.LogInformation("Reading latest data from Modbus API...");

            HttpClient client = _httpClientFactory.CreateClient("Modbus");

            List<SensorReading>? readings = null;

            for (int attempt = 1; attempt <= MaxAttempts; attempt++)
            {
                try
                {
                    _logger.LogInformation(
                        "Reading Modbus data, attempt {Attempt}/{MaxAttempts}.",
                        attempt,
                        MaxAttempts
                    );

                    readings = await client.GetFromJsonAsync<List<SensorReading>>(
                        "/api/readings",
                        cancellationToken
                    );

                    // Đọc thành công
                    if (readings is { Count: > 0 })
                    {
                        _logger.LogInformation(
                            "Modbus read succeeded on attempt {Attempt}/{MaxAttempts}.",
                            attempt,
                            MaxAttempts
                        );

                        break;
                    }

                    // API trả về rỗng -> coi là lần đọc không thành công
                    _logger.LogWarning(
                        "Modbus API returned no readings on attempt {Attempt}/{MaxAttempts}.",
                        attempt,
                        MaxAttempts
                    );
                }
                catch (HttpRequestException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Modbus read failed on attempt {Attempt}/{MaxAttempts}.",
                        attempt,
                        MaxAttempts
                    );
                }
                catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning(
                        "Modbus read timeout on attempt {Attempt}/{MaxAttempts}.",
                        attempt,
                        MaxAttempts
                    );
                }

                // Nếu chưa phải lần cuối thì chờ 500 ms rồi thử lại
                if (attempt < MaxAttempts)
                {
                    await Task.Delay(RetryDelayMilliseconds, cancellationToken);
                }
            }

            // Cả 5 lần đều không lấy được dữ liệu
            if (readings is not { Count: > 0 })
            {
                _logger.LogError(
                    "Failed to read Modbus data after {MaxAttempts} attempts.",
                    MaxAttempts
                );

                return;
            }

            _logger.LogInformation("Received {Count} sensor readings from Modbus.", readings.Count);

            foreach (SensorReading reading in readings)
            {
                _logger.LogDebug(
                    "Modbus Data | Station={Station} | SlaveId={SlaveId} | Parameter={Parameter} | Value={Value} {Unit} | Status={Status} | Timestamp={Timestamp}",
                    reading.StationName,
                    reading.SlaveId,
                    reading.Name,
                    reading.Value,
                    reading.Unit,
                    reading.Status,
                    reading.Timestamp
                );
            }

            List<StationMessage> messages = BuildStationMessages(readings);

            if (messages.Count == 0)
            {
                _logger.LogWarning("No station messages were created from Modbus readings.");

                return;
            }

            int queued = _queueService.EnqueueStationMessages(messages);

            _logger.LogInformation(
                "Added {Count} station messages to MQTT, FTP and FTP2 RAM queues.",
                queued
            );
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process Modbus data.");
        }
    }

    // =============================================================
    // BUILD STATION MESSAGES
    // =============================================================

    private static List<StationMessage> BuildStationMessages(List<SensorReading> readings)
    {
        string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");

        return readings
            .Where(x => !string.IsNullOrWhiteSpace(x.StationName))
            .GroupBy(x => x.StationName, StringComparer.OrdinalIgnoreCase)
            .Select(group => new StationMessage
            {
                Timestamp = timestamp,
                StationName = group.Key,

                Measurements = group.Select(CloneReading).ToList(),
            })
            .ToList();
    }

    // =============================================================
    // CLONE READING
    // =============================================================

    private static SensorReading CloneReading(SensorReading reading)
    {
        return new SensorReading
        {
            StationName = reading.StationName,
            SlaveId = reading.SlaveId,
            Name = reading.Name,
            Value = reading.Value,
            Unit = reading.Unit,
            Status = reading.Status,
            Timestamp = reading.Timestamp,
        };
    }

    // =============================================================
    // CLEANUP DATABASE
    // =============================================================

    private void CleanupDatabase()
    {
        try
        {
            _logger.LogInformation("Starting database cleanup...");

            DatabaseCleanupResult result = _databaseService.CleanupDatabase(
                _settings.Database.RetentionDays,
                _settings.Database.MaxDatabaseSizeMB,
                _settings.Database.VacuumAfterCleanup
            );

            _logger.LogInformation(
                "Database cleanup completed. "
                    + "DeletedByRetention={DeletedByRetention}, "
                    + "DeletedBySize={DeletedBySize}, "
                    + "SizeBefore={SizeBeforeBytes} bytes, "
                    + "SizeAfter={SizeAfterBytes} bytes.",
                result.DeletedByRetention,
                result.DeletedBySize,
                result.SizeBeforeBytes,
                result.SizeAfterBytes
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to cleanup database.");
        }
    }

    // =============================================================
    // CALCULATE NEXT SAVE TIME
    // =============================================================

    private DateTime CalculateNextSaveTime(DateTime now)
    {
        int intervalMinutes = _settings.Database.SaveIntervalMinutes;

        int saveSecond = _settings.Database.SaveSecond;

        DateTime currentMinute = new DateTime(
            now.Year,
            now.Month,
            now.Day,
            now.Hour,
            now.Minute,
            0
        );

        DateTime candidate = currentMinute.AddSeconds(saveSecond);

        if (candidate <= now)
        {
            candidate = candidate.AddMinutes(intervalMinutes);
        }

        return candidate;
    }

    // =============================================================
    // VALIDATE SETTINGS
    // =============================================================

    private void ValidateSettings()
    {
        if (_settings.Database.SaveIntervalMinutes <= 0)
        {
            throw new InvalidOperationException(
                "Database.SaveIntervalMinutes must be greater than 0."
            );
        }

        if (_settings.Database.SaveSecond < 0 || _settings.Database.SaveSecond > 59)
        {
            throw new InvalidOperationException("Database.SaveSecond must be between 0 and 59.");
        }

        if (_settings.Database.RetentionDays <= 0)
        {
            throw new InvalidOperationException("Database.RetentionDays must be greater than 0.");
        }

        if (_settings.Database.MaxDatabaseSizeMB <= 0)
        {
            throw new InvalidOperationException(
                "Database.MaxDatabaseSizeMB must be greater than 0."
            );
        }

        if (_settings.Database.CleanupIntervalSeconds <= 0)
        {
            throw new InvalidOperationException(
                "Database.CleanupIntervalSeconds must be greater than 0."
            );
        }

        if (string.IsNullOrWhiteSpace(_settings.ModbusApi.BaseUrl))
        {
            throw new InvalidOperationException("ModbusApi.BaseUrl cannot be empty.");
        }
    }
}
