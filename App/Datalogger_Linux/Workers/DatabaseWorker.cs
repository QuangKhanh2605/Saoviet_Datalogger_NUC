using Datalogger_Linux.Models;
using Datalogger_Linux.Services;
using Microsoft.Extensions.Options;

namespace Datalogger_Linux.Workers;

public class DatabaseWorker : BackgroundService
{
    private readonly ILogger<DatabaseWorker> _logger;

    private readonly DatabaseService _database;

    private readonly ReadingStore _readingStore;

    private readonly StationMessageService _stationMessageService;

    private readonly Configuration.AppSettings _settings;

    public DatabaseWorker(
        ILogger<DatabaseWorker> logger,
        DatabaseService database,
        ReadingStore readingStore,
        StationMessageService stationMessageService,
        IOptions<Configuration.AppSettings> options
    )
    {
        _logger = logger;

        _database = database;

        _readingStore = readingStore;

        _stationMessageService = stationMessageService;

        _settings = options.Value;
    }

    // =========================================================
    // EXECUTE
    // =========================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ValidateSettings();

        // FIRST SAVE
        DateTime firstSaveTime = DateTime.Now.AddMinutes(1);

        DateTime nextSaveTime = firstSaveTime;

        DateTime nextCleanupTime = DateTime.Now.AddSeconds(
            _settings.Database.CleanupIntervalSeconds
        );

        bool firstSavePending = true;

        _logger.LogInformation(
            "DatabaseWorker started. "
                + "FirstSave={FirstSave:yyyy-MM-dd HH:mm:ss} | "
                + "SaveInterval={SaveInterval} minutes | "
                + "SaveSecond={SaveSecond:00} | "
                + "CleanupInterval={CleanupInterval}s",
            firstSaveTime,
            _settings.Database.SaveIntervalMinutes,
            _settings.Database.SaveSecond,
            _settings.Database.CleanupIntervalSeconds
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                DateTime now = DateTime.Now;

                // =================================================
                // SAVE
                // =================================================

                if (now >= nextSaveTime)
                {
                    try
                    {
                        SaveCurrentReadings();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Lỗi lưu dữ liệu vào SQLite.");
                    }

                    if (firstSavePending)
                    {
                        firstSavePending = false;

                        nextSaveTime = CalculateNextSaveTime(DateTime.Now);

                        _logger.LogInformation(
                            "SQLite first save completed. "
                                + "Next scheduled save: {NextSave:yyyy-MM-dd HH:mm:ss}",
                            nextSaveTime
                        );
                    }
                    else
                    {
                        nextSaveTime = CalculateNextSaveTime(DateTime.Now);
                    }
                }

                // =================================================
                // CLEANUP
                // =================================================

                if (now >= nextCleanupTime)
                {
                    try
                    {
                        CleanupDatabase();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Lỗi cleanup database.");
                    }

                    nextCleanupTime = DateTime.Now.AddSeconds(
                        _settings.Database.CleanupIntervalSeconds
                    );
                }

                // =================================================
                // CALCULATE NEXT WAKE
                // =================================================

                DateTime nextEvent =
                    nextSaveTime < nextCleanupTime ? nextSaveTime : nextCleanupTime;

                TimeSpan delay = nextEvent - DateTime.Now;

                if (delay < TimeSpan.FromMilliseconds(100))
                {
                    delay = TimeSpan.FromMilliseconds(100);
                }

                if (delay > TimeSpan.FromSeconds(1))
                {
                    delay = TimeSpan.FromSeconds(1);
                }

                await Task.Delay(delay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DatabaseWorker gặp lỗi không mong muốn.");

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

    // =========================================================
    // SAVE CURRENT READINGS
    // =========================================================

    private void SaveCurrentReadings()
    {
        List<SensorReading> readings = _readingStore.GetLatestReadings();

        if (readings.Count == 0)
        {
            _logger.LogInformation("Chưa có dữ liệu để lưu SQLite.");

            return;
        }

        List<StationMessage> messages = _stationMessageService.BuildStationMessages(readings);

        if (messages.Count == 0)
        {
            return;
        }

        // =====================================================
        // TIMESTAMP DATABASE MESSAGE
        // =====================================================

        string saveTimestamp = DateTime.Now.ToString("yyyyMMddHHmmss");

        foreach (StationMessage message in messages)
        {
            message.Timestamp = saveTimestamp;
        }

        // =====================================================
        // SAVE
        // =====================================================

        _database.InsertStationMessages(messages);

        // =====================================================
        // LOG
        // =====================================================

        foreach (StationMessage message in messages)
        {
            _logger.LogInformation(
                "SQLite | "
                    + "Message={MessageId} | "
                    + "Station={Station} | "
                    + "Timestamp={Timestamp} | "
                    + "Measurements={Count}",
                message.Id,
                message.StationName,
                message.Timestamp,
                message.Measurements.Count
            );
        }
    }

    // =========================================================
    // CLEANUP DATABASE
    // =========================================================

    private void CleanupDatabase()
    {
        int retentionDays = _settings.Database.RetentionDays;

        int maxDatabaseSizeMB = _settings.Database.MaxDatabaseSizeMB;

        int cleanupIntervalSeconds = _settings.Database.CleanupIntervalSeconds;

        if (retentionDays <= 0)
        {
            _logger.LogWarning("RetentionDays phải lớn hơn 0.");

            return;
        }

        if (maxDatabaseSizeMB <= 0)
        {
            _logger.LogWarning("MaxDatabaseSizeMB phải lớn hơn 0.");

            return;
        }

        if (cleanupIntervalSeconds <= 0)
        {
            _logger.LogWarning("CleanupIntervalSeconds phải lớn hơn 0.");

            return;
        }

        DatabaseCleanupResult result = _database.CleanupDatabase(
            retentionDays,
            maxDatabaseSizeMB,
            _settings.Database.VacuumAfterCleanup
        );

        double beforeMB = result.SizeBeforeBytes / 1024.0 / 1024.0;

        double afterMB = result.SizeAfterBytes / 1024.0 / 1024.0;

        if (result.TotalDeleted > 0)
        {
            _logger.LogInformation(
                "SQLite cleanup | "
                    + "Deleted={Deleted} | "
                    + "RetentionDeleted={RetentionDeleted} | "
                    + "SizeDeleted={SizeDeleted} | "
                    + "SizeBefore={BeforeMB:F2} MB | "
                    + "SizeAfter={AfterMB:F2} MB | "
                    + "Limit={LimitMB} MB",
                result.TotalDeleted,
                result.DeletedByRetention,
                result.DeletedBySize,
                beforeMB,
                afterMB,
                maxDatabaseSizeMB
            );
        }
        else
        {
            _logger.LogInformation(
                "SQLite cleanup | "
                    + "Size={SizeMB:F2} MB | "
                    + "Limit={LimitMB} MB | "
                    + "No cleanup needed.",
                afterMB,
                maxDatabaseSizeMB
            );
        }
    }

    // =========================================================
    // VALIDATE SETTINGS
    // =========================================================

    private void ValidateSettings()
    {
        int saveIntervalMinutes = _settings.Database.SaveIntervalMinutes;

        int saveSecond = _settings.Database.SaveSecond;

        int cleanupIntervalSeconds = _settings.Database.CleanupIntervalSeconds;

        if (saveIntervalMinutes < 1 || saveIntervalMinutes > 60)
        {
            throw new InvalidOperationException(
                "Database.SaveIntervalMinutes " + "phải nằm trong khoảng 1..60."
            );
        }

        if (saveSecond < 0 || saveSecond > 59)
        {
            throw new InvalidOperationException(
                "Database.SaveSecond " + "phải nằm trong khoảng 0..59."
            );
        }

        if (cleanupIntervalSeconds <= 0)
        {
            throw new InvalidOperationException(
                "Database.CleanupIntervalSeconds " + "phải lớn hơn 0."
            );
        }

        if (_settings.Database.RetentionDays <= 0)
        {
            throw new InvalidOperationException("Database.RetentionDays " + "phải lớn hơn 0.");
        }

        if (_settings.Database.MaxDatabaseSizeMB <= 0)
        {
            throw new InvalidOperationException("Database.MaxDatabaseSizeMB " + "phải lớn hơn 0.");
        }
    }

    // =========================================================
    // CALCULATE NEXT SAVE TIME
    // =========================================================

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
            0,
            now.Kind
        );

        // =====================================================
        // CURRENT MINUTE IS VALID
        // =====================================================

        if (now.Minute % intervalMinutes == 0)
        {
            DateTime candidate = currentMinute.AddSeconds(saveSecond);

            if (candidate > now)
            {
                return candidate;
            }
        }

        // =====================================================
        // FIND NEXT VALID MINUTE
        // =====================================================

        int remainder = now.Minute % intervalMinutes;

        int minutesToNext = intervalMinutes - remainder;

        if (minutesToNext == 0)
        {
            minutesToNext = intervalMinutes;
        }

        DateTime nextMinute = currentMinute.AddMinutes(minutesToNext);

        return new DateTime(
            nextMinute.Year,
            nextMinute.Month,
            nextMinute.Day,
            nextMinute.Hour,
            nextMinute.Minute,
            saveSecond,
            nextMinute.Kind
        );
    }
}
