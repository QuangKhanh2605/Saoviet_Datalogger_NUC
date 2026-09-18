using System.Text;
using Datalogger_Linux.Configuration;
using Datalogger_Linux.Ftp;
using Datalogger_Linux.Models;
using Datalogger_Linux.Services;
using Microsoft.Extensions.Options;

namespace Datalogger_Linux.Workers;

public class FtpWorker : BackgroundService
{
    private const int MaxMessagesPerFile = 50;

    private readonly ILogger<FtpWorker> _logger;
    private readonly DatabaseService _database;
    private readonly FtpService _ftpService;

    private readonly FtpSettings _ftpSettings;
    private readonly FtpSettings _ftp2Settings;

    public FtpWorker(
        ILogger<FtpWorker> logger,
        DatabaseService database,
        FtpService ftpService,
        IOptions<AppSettings> options
    )
    {
        _logger = logger;
        _database = database;
        _ftpService = ftpService;

        _ftpSettings = options.Value.Ftp;
        _ftp2Settings = options.Value.Ftp2;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("FTP Worker started.");

        // =====================================================
        // VALIDATE ENABLED SERVICES
        // =====================================================

        if (!_ftpSettings.Enabled && !_ftp2Settings.Enabled)
        {
            _logger.LogInformation("FTP1 and FTP2 are both disabled.");

            return;
        }

        // =====================================================
        // VALIDATE FTP1
        // =====================================================

        if (_ftpSettings.Enabled)
        {
            if (_ftpSettings.Servers.Count == 0)
            {
                _logger.LogWarning("FTP1 enabled nhưng không có server.");
            }
            else
            {
                ValidateServers(_ftpSettings, "FTP1");
            }
        }

        // =====================================================
        // VALIDATE FTP2
        // =====================================================

        if (_ftp2Settings.Enabled)
        {
            if (_ftp2Settings.Servers.Count == 0)
            {
                _logger.LogWarning("FTP2 enabled nhưng không có server.");
            }
            else
            {
                ValidateServers(_ftp2Settings, "FTP2");
            }
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                bool processedAnyData = false;

                // =================================================
                // FTP1
                // =================================================

                if (_ftpSettings.Enabled && _ftpSettings.Servers.Count > 0)
                {
                    foreach (FtpServerSettings server in _ftpSettings.Servers)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            break;

                        bool processed = await ProcessServerAsync(
                            server,
                            ftp2: false,
                            stoppingToken
                        );

                        if (processed)
                            processedAnyData = true;
                    }
                }

                // =================================================
                // FTP2
                // =================================================

                if (_ftp2Settings.Enabled && _ftp2Settings.Servers.Count > 0)
                {
                    foreach (FtpServerSettings server in _ftp2Settings.Servers)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            break;

                        bool processed = await ProcessServerAsync(
                            server,
                            ftp2: true,
                            stoppingToken
                        );

                        if (processed)
                            processedAnyData = true;
                    }
                }

                // =================================================
                // CONTINUE IMMEDIATELY IF DATA WAS PROCESSED
                // =================================================

                if (processedAnyData)
                    continue;

                // =================================================
                // NO DATA
                // =================================================

                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("FTP Worker stopping.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FTP Worker stopped unexpectedly.");
        }

        _logger.LogInformation("FTP Worker stopped.");
    }

    // ============================================================
    // PROCESS SERVER
    // ============================================================

    private async Task<bool> ProcessServerAsync(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken
    )
    {
        return server.PackageMode switch
        {
            1 => await ProcessMode1Async(server, ftp2, stoppingToken),

            2 => await ProcessMode2Async(server, ftp2, stoppingToken),

            3 => await ProcessMode3Async(server, ftp2, stoppingToken),

            _ => throw new ArgumentOutOfRangeException(
                nameof(server.PackageMode),
                server.PackageMode,
                "PackageMode phải là 1, 2 hoặc 3."
            ),
        };
    }

    // ============================================================
    // MODE 1
    // ============================================================

    private async Task<bool> ProcessMode1Async(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken
    )
    {
        List<StationMessage> messages = GetPendingFtpMessages(server.KYHIEU_TRAM, ftp2);

        if (messages.Count == 0)
            return false;

        bool processedAny = false;

        foreach (StationMessage message in messages)
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            bool allUploaded = true;

            foreach (SensorReading reading in message.Measurements)
            {
                // -------------------------------------------------
                // Thời điểm đóng gói file.
                //
                // Dùng cho:
                // 1. Tên file
                // 2. ModePath
                //
                // Không dùng message.Timestamp ở đây.
                // -------------------------------------------------

                DateTime packageTimestamp = DateTime.Now;

                string fileName = BuildMode1FileName(
                    server,
                    reading.ParameterName,
                    packageTimestamp
                );

                string content = BuildMode1Content(message, reading);

                byte[] data = Encoding.UTF8.GetBytes(content);

                bool success = await _ftpService.UploadBytesAsync(
                    server,
                    fileName,
                    data,
                    packageTimestamp,
                    stoppingToken
                );

                if (!success)
                {
                    allUploaded = false;

                    _logger.LogWarning(
                        "{FtpType} Mode 1 upload thất bại. "
                            + "MessageId={MessageId}, "
                            + "Station={Station}, "
                            + "Parameter={Parameter}",
                        GetFtpTypeName(ftp2),
                        message.Id,
                        message.StationName,
                        reading.ParameterName
                    );

                    break;
                }
            }

            if (allUploaded)
            {
                MarkFtpSent(new[] { message.Id }, ftp2);

                processedAny = true;

                _logger.LogInformation(
                    "{FtpType} Mode 1 hoàn thành. " + "MessageId={MessageId}",
                    GetFtpTypeName(ftp2),
                    message.Id
                );
            }
        }

        return processedAny;
    }

    // ============================================================
    // MODE 2
    // ============================================================

    private async Task<bool> ProcessMode2Async(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken
    )
    {
        List<StationMessage> messages = GetPendingFtpMessages(server.KYHIEU_TRAM, ftp2);

        if (messages.Count == 0)
            return false;

        // ---------------------------------------------------------
        // Một package = một thời điểm đóng gói.
        //
        // Timestamp này được dùng chung cho:
        // 1. Tên file
        // 2. ModePath
        // ---------------------------------------------------------

        DateTime packageTimestamp = DateTime.Now;

        string fileName = BuildMode2FileName(server, packageTimestamp);

        string content = BuildMode2Content(messages);

        byte[] data = Encoding.UTF8.GetBytes(content);

        bool success = await _ftpService.UploadBytesAsync(
            server,
            fileName,
            data,
            packageTimestamp,
            stoppingToken
        );

        if (!success)
        {
            _logger.LogWarning(
                "{FtpType} Mode 2 upload thất bại. " + "Station={Station}, Messages={Count}",
                GetFtpTypeName(ftp2),
                server.KYHIEU_TRAM,
                messages.Count
            );

            return false;
        }

        MarkFtpSent(messages.Select(x => x.Id), ftp2);

        _logger.LogInformation(
            "{FtpType} Mode 2 hoàn thành. " + "Station={Station}, Messages={Count}",
            GetFtpTypeName(ftp2),
            server.KYHIEU_TRAM,
            messages.Count
        );

        return true;
    }

    // ============================================================
    // MODE 3
    // ============================================================

    private async Task<bool> ProcessMode3Async(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken
    )
    {
        FtpSettings settings = ftp2 ? _ftp2Settings : _ftpSettings;

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
            return false;

        List<StationMessage> messages = GetPendingFtpMessages(stationNames, ftp2);

        if (messages.Count == 0)
            return false;

        // ---------------------------------------------------------
        // Một package = một thời điểm đóng gói.
        //
        // Timestamp này được dùng chung cho:
        // 1. Tên file
        // 2. ModePath
        // ---------------------------------------------------------

        DateTime packageTimestamp = DateTime.Now;

        string fileName = BuildMode3FileName(server, packageTimestamp);

        string content = BuildMode3Content(messages);

        byte[] data = Encoding.UTF8.GetBytes(content);

        bool success = await _ftpService.UploadBytesAsync(
            server,
            fileName,
            data,
            packageTimestamp,
            stoppingToken
        );

        if (!success)
        {
            _logger.LogWarning(
                "{FtpType} Mode 3 upload thất bại. " + "CongTrinh={CongTrinh}, Messages={Count}",
                GetFtpTypeName(ftp2),
                server.KYHIEU_CONGTRINH,
                messages.Count
            );

            return false;
        }

        MarkFtpSent(messages.Select(x => x.Id), ftp2);

        _logger.LogInformation(
            "{FtpType} Mode 3 hoàn thành. " + "CongTrinh={CongTrinh}, Messages={Count}",
            GetFtpTypeName(ftp2),
            server.KYHIEU_CONGTRINH,
            messages.Count
        );

        return true;
    }

    // ============================================================
    // GET PENDING FTP MESSAGES
    // ============================================================

    private List<StationMessage> GetPendingFtpMessages(string stationName, bool ftp2)
    {
        if (ftp2)
        {
            return _database.GetPendingFtp2Messages(new[] { stationName }, MaxMessagesPerFile);
        }

        return _database.GetPendingFtpMessages(new[] { stationName }, MaxMessagesPerFile);
    }

    // ============================================================
    // GET PENDING FTP MESSAGES - MULTIPLE STATIONS
    // ============================================================

    private List<StationMessage> GetPendingFtpMessages(IEnumerable<string> stationNames, bool ftp2)
    {
        if (ftp2)
        {
            return _database.GetPendingFtp2Messages(stationNames, MaxMessagesPerFile);
        }

        return _database.GetPendingFtpMessages(stationNames, MaxMessagesPerFile);
    }

    // ============================================================
    // MARK FTP SENT
    // ============================================================

    private void MarkFtpSent(IEnumerable<long> messageIds, bool ftp2)
    {
        if (ftp2)
        {
            _database.MarkFtp2Sent(messageIds);

            return;
        }

        _database.MarkFtpSent(messageIds);
    }

    // ============================================================
    // FTP TYPE NAME
    // ============================================================

    private static string GetFtpTypeName(bool ftp2)
    {
        return ftp2 ? "FTP2" : "FTP1";
    }

    // ============================================================
    // FILENAME - MODE 1
    // ============================================================

    private static string BuildMode1FileName(
        FtpServerSettings server,
        string parameterName,
        DateTime packageTimestamp
    )
    {
        string sendTime = packageTimestamp.ToString("yyyyMMddHHmmss");

        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{server.KYHIEU_TRAM}_"
            + $"{parameterName}_"
            + $"{sendTime}.txt";
    }

    // ============================================================
    // FILENAME - MODE 2
    // ============================================================

    private static string BuildMode2FileName(FtpServerSettings server, DateTime packageTimestamp)
    {
        string sendTime = packageTimestamp.ToString("yyyyMMddHHmmss");

        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{server.KYHIEU_TRAM}_"
            + $"{sendTime}.txt";
    }

    // ============================================================
    // FILENAME - MODE 3
    // ============================================================

    private static string BuildMode3FileName(FtpServerSettings server, DateTime packageTimestamp)
    {
        string sendTime = packageTimestamp.ToString("yyyyMMddHHmmss");

        return $"{server.MA_TINH}_" + $"{server.KYHIEU_CONGTRINH}_" + $"{sendTime}.txt";
    }

    // ============================================================
    // CONTENT - MODE 1
    // ============================================================

    private static string BuildMode1Content(StationMessage message, SensorReading reading)
    {
        // message.Timestamp =
        // thời điểm THU THẬP dữ liệu.
        //
        // Timestamp này nằm trong nội dung file.

        return $"{message.Timestamp}\t"
            + $"{reading.Value}\t"
            + $"{reading.Unit}\t"
            + $"{reading.Status:D2}"
            + Environment.NewLine;
    }

    // ============================================================
    // CONTENT - MODE 2
    // ============================================================

    private static string BuildMode2Content(IEnumerable<StationMessage> messages)
    {
        var builder = new StringBuilder();

        foreach (StationMessage message in messages)
        {
            foreach (SensorReading reading in message.Measurements)
            {
                builder.AppendLine(
                    $"{reading.ParameterName}\t"
                        + $"{reading.Value}\t"
                        + $"{reading.Unit}\t"
                        + $"{message.Timestamp}\t"
                        + $"{reading.Status:D2}"
                );
            }
        }

        return builder.ToString();
    }

    // ============================================================
    // CONTENT - MODE 3
    // ============================================================

    private static string BuildMode3Content(IEnumerable<StationMessage> messages)
    {
        var builder = new StringBuilder();

        foreach (StationMessage message in messages)
        {
            foreach (SensorReading reading in message.Measurements)
            {
                builder.AppendLine(
                    $"{message.StationName}\t"
                        + $"{reading.ParameterName}\t"
                        + $"{reading.Value}\t"
                        + $"{reading.Unit}\t"
                        + $"{message.Timestamp}\t"
                        + $"{reading.Status:D2}"
                );
            }
        }

        return builder.ToString();
    }

    // ============================================================
    // VALIDATE FTP SERVER CONFIGURATION
    // ============================================================

    private void ValidateServers(FtpSettings settings, string ftpType)
    {
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
}
