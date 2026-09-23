using System.Net.Http.Json;
using Database.Configuration;
using Database.Models;
using Database.Services;
using Microsoft.Extensions.Options;

namespace Database.Workers;

public class DatabaseWorker : BackgroundService
{
    private readonly ILogger<DatabaseWorker> _logger;
    private readonly DatabaseService _databaseService;
    private readonly AppSettings _settings;
    private readonly IHttpClientFactory _httpClientFactory;

    public DatabaseWorker(
        ILogger<DatabaseWorker> logger,
        DatabaseService databaseService,
        IOptions<AppSettings> settings,
        IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _databaseService = databaseService;
        _settings = settings.Value;
        _httpClientFactory = httpClientFactory;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "DatabaseWorker started.");

        ValidateSettings();

        DateTime nextSaveTime =
            CalculateNextSaveTime(
                DateTime.Now);

        DateTime nextCleanupTime =
            DateTime.Now.AddSeconds(
                _settings.Database.CleanupIntervalSeconds);

        _logger.LogInformation(
            "Next database save scheduled at {NextSaveTime}.",
            nextSaveTime);

        _logger.LogInformation(
            "Next database cleanup scheduled at {NextCleanupTime}.",
            nextCleanupTime);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                DateTime now = DateTime.Now;

                // =====================================================
                // SAVE DATA
                // =====================================================

                if (now >= nextSaveTime)
                {
                    await SaveModbusDataAsync(
                        stoppingToken);

                    nextSaveTime =
                        CalculateNextSaveTime(
                            DateTime.Now);
                }

                // =====================================================
                // CLEANUP DATABASE
                // =====================================================

                if (now >= nextCleanupTime)
                {
                    CleanupDatabase();

                    nextCleanupTime =
                        DateTime.Now.AddSeconds(
                            _settings.Database.CleanupIntervalSeconds);
                }

                // =====================================================
                // CALCULATE NEXT WAKE-UP
                // =====================================================

                DateTime nextEvent =
                    nextSaveTime < nextCleanupTime
                        ? nextSaveTime
                        : nextCleanupTime;

                TimeSpan delay =
                    nextEvent - DateTime.Now;

                if (delay <= TimeSpan.Zero)
                {
                    continue;
                }

                // Không cần kiểm tra liên tục.
                // Worker ngủ cho tới gần thời điểm cần xử lý.
                await Task.Delay(
                    delay,
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Unexpected error in DatabaseWorker.");

                // Nếu có lỗi, không để worker chạy vòng lặp
                // quá nhanh gây CPU load.
                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(1),
                        stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        _logger.LogInformation(
            "DatabaseWorker stopped.");
    }

    // =============================================================
    // READ MODBUS + SAVE DATABASE
    // =============================================================

    private async Task SaveModbusDataAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Reading latest data from Modbus API...");

            HttpClient client =
                _httpClientFactory.CreateClient("Modbus");

            List<SensorReading>? readings =
                await client.GetFromJsonAsync<List<SensorReading>>(
                    "/api/readings",
                    cancellationToken);

            if (readings == null || readings.Count == 0)
            {
                _logger.LogWarning(
                    "Modbus API returned no readings.");

                return;
            }

            _logger.LogInformation(
                "Received {Count} sensor readings from Modbus.",
                readings.Count);

            foreach (SensorReading reading in readings)
            {
                _logger.LogInformation(
                    "Modbus Data | Station={Station} | SlaveId={SlaveId} | Parameter={Parameter} | Value={Value} {Unit} | Status={Status} | Timestamp={Timestamp}",
                    reading.StationName,
                    reading.SlaveId,
                    reading.ParameterName,
                    reading.Value,
                    reading.Unit,
                    reading.Status,
                    reading.Timestamp);
            }

            List<StationMessage> messages =
                BuildStationMessages(readings);

            if (messages.Count == 0)
            {
                _logger.LogWarning(
                    "No station messages were created from Modbus readings.");

                return;
            }

            _databaseService.InsertStationMessages(
                messages);

            _logger.LogInformation(
                "Saved {StationCount} station messages to database.",
                messages.Count);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(
                ex,
                "Failed to read data from Modbus API: {ModbusApiUrl}",
                _settings.ModbusApi.BaseUrl);
        }
        catch (TaskCanceledException) when (
            !cancellationToken.IsCancellationRequested)
        {
            _logger.LogError(
                "Timeout while reading Modbus API.");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to save Modbus data to database.");
        }
    }

    // =============================================================
    // BUILD STATION MESSAGES
    // =============================================================

    private static List<StationMessage> BuildStationMessages(
        List<SensorReading> readings)
    {
        string timestamp =
            DateTime.Now.ToString("yyyyMMddHHmmss");

        return readings
            .Where(x =>
                !string.IsNullOrWhiteSpace(x.StationName))
            .GroupBy(
                x => x.StationName,
                StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                return new StationMessage
                {
                    Timestamp = timestamp,

                    StationName = group.Key,

                    MqttSent = false,

                    FtpSent = false,

                    Ftp2Sent = false,

                    Measurements = group
                        .Select(CloneReading)
                        .ToList()
                };
            })
            .ToList();
    }

    // =============================================================
    // CLONE READING
    // =============================================================

    private static SensorReading CloneReading(
        SensorReading reading)
    {
        return new SensorReading
        {
            StationName = reading.StationName,

            SlaveId = reading.SlaveId,

            ParameterName = reading.ParameterName,

            Value = reading.Value,

            Unit = reading.Unit,

            Status = reading.Status,

            Timestamp = reading.Timestamp
        };
    }

    // =============================================================
    // CLEANUP
    // =============================================================

    private void CleanupDatabase()
    {
        try
        {
            _logger.LogInformation(
                "Starting database cleanup...");

            DatabaseCleanupResult result =
                _databaseService.CleanupDatabase(
                    _settings.Database.RetentionDays,
                    _settings.Database.MaxDatabaseSizeMB,
                    _settings.Database.VacuumAfterCleanup);

            _logger.LogInformation(
                "Database cleanup completed. " +
                "DeletedByRetention={DeletedByRetention}, " +
                "DeletedBySize={DeletedBySize}, " +
                "SizeBefore={SizeBeforeBytes} bytes, " +
                "SizeAfter={SizeAfterBytes} bytes.",
                result.DeletedByRetention,
                result.DeletedBySize,
                result.SizeBeforeBytes,
                result.SizeAfterBytes);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to cleanup database.");
        }
    }

    // =============================================================
    // CALCULATE NEXT SAVE TIME
    // =============================================================

    private DateTime CalculateNextSaveTime(
        DateTime now)
    {
        int intervalMinutes =
            _settings.Database.SaveIntervalMinutes;

        int saveSecond =
            _settings.Database.SaveSecond;

        DateTime currentMinute =
            new DateTime(
                now.Year,
                now.Month,
                now.Day,
                now.Hour,
                now.Minute,
                0);

        DateTime candidate =
            currentMinute.AddSeconds(
                saveSecond);

        if (candidate <= now)
        {
            candidate =
                candidate.AddMinutes(
                    intervalMinutes);
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
                "Database.SaveIntervalMinutes must be greater than 0.");
        }

        if (_settings.Database.SaveSecond < 0 ||
            _settings.Database.SaveSecond > 59)
        {
            throw new InvalidOperationException(
                "Database.SaveSecond must be between 0 and 59.");
        }

        if (_settings.Database.RetentionDays <= 0)
        {
            throw new InvalidOperationException(
                "Database.RetentionDays must be greater than 0.");
        }

        if (_settings.Database.MaxDatabaseSizeMB <= 0)
        {
            throw new InvalidOperationException(
                "Database.MaxDatabaseSizeMB must be greater than 0.");
        }

        if (_settings.Database.CleanupIntervalSeconds <= 0)
        {
            throw new InvalidOperationException(
                "Database.CleanupIntervalSeconds must be greater than 0.");
        }

        if (string.IsNullOrWhiteSpace(
                _settings.ModbusApi.BaseUrl))
        {
            throw new InvalidOperationException(
                "ModbusApi.BaseUrl cannot be empty.");
        }
    }
}