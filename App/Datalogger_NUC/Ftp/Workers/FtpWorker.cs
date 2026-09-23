using System.Text;
using Ftp.Configuration;
using Ftp.Models;
using Ftp.Services;
using Microsoft.Extensions.Options;

namespace Ftp.Workers;

public sealed class FtpWorker : BackgroundService
{
    private const int MaxMessagesPerFile = 50;

    private readonly ILogger<FtpWorker> _logger;
    private readonly DatabaseApiService _databaseApi;
    private readonly FtpService _ftpService;

    private readonly FtpSettings _ftpSettings;
    private readonly FtpSettings _ftp2Settings;

    private volatile bool _workerRunning;

    private DateTime? _lastSuccessfulProcess;
    private DateTime? _lastErrorTime;
    private string? _lastError;

    public FtpWorker(
        ILogger<FtpWorker> logger,
        DatabaseApiService databaseApi,
        FtpService ftpService,
        IOptions<AppSettings> options)
    {
        _logger = logger;
        _databaseApi = databaseApi;
        _ftpService = ftpService;

        _ftpSettings = options.Value.Ftp;
        _ftp2Settings = options.Value.Ftp2;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "FTP Worker started.");

        _workerRunning = true;

        if (!_ftpSettings.Enabled &&
            !_ftp2Settings.Enabled)
        {
            _logger.LogInformation(
                "FTP1 and FTP2 are both disabled.");

            _workerRunning = false;

            return;
        }

        if (_ftpSettings.Enabled)
        {
            if (_ftpSettings.Servers.Count == 0)
            {
                _logger.LogWarning(
                    "FTP1 enabled nhưng không có server.");
            }
            else
            {
                ValidateServers(
                    _ftpSettings,
                    "FTP1");
            }
        }

        if (_ftp2Settings.Enabled)
        {
            if (_ftp2Settings.Servers.Count == 0)
            {
                _logger.LogWarning(
                    "FTP2 enabled nhưng không có server.");
            }
            else
            {
                ValidateServers(
                    _ftp2Settings,
                    "FTP2");
            }
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                bool processedAnyData = false;

                if (_ftpSettings.Enabled &&
                    _ftpSettings.Servers.Count > 0)
                {
                    foreach (FtpServerSettings server
                             in _ftpSettings.Servers)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            break;

                        bool processed =
                            await ProcessServerAsync(
                                server,
                                false,
                                stoppingToken);

                        if (processed)
                            processedAnyData = true;
                    }
                }

                if (_ftp2Settings.Enabled &&
                    _ftp2Settings.Servers.Count > 0)
                {
                    foreach (FtpServerSettings server
                             in _ftp2Settings.Servers)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            break;

                        bool processed =
                            await ProcessServerAsync(
                                server,
                                true,
                                stoppingToken);

                        if (processed)
                            processedAnyData = true;
                    }
                }

                if (processedAnyData)
                    continue;

                await Task.Delay(
                    TimeSpan.FromSeconds(1),
                    stoppingToken);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "FTP Worker stopping.");
        }
        catch (Exception ex)
        {
            _lastErrorTime = DateTime.Now;
            _lastError = ex.Message;

            _logger.LogError(
                ex,
                "FTP Worker stopped unexpectedly.");
        }

        _workerRunning = false;

        _logger.LogInformation(
            "FTP Worker stopped.");
    }

    private async Task<bool> ProcessServerAsync(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken)
    {
        return server.PackageMode switch
        {
            1 => await ProcessMode1Async(
                server,
                ftp2,
                stoppingToken),

            2 => await ProcessMode2Async(
                server,
                ftp2,
                stoppingToken),

            3 => await ProcessMode3Async(
                server,
                ftp2,
                stoppingToken),

            _ => throw new ArgumentOutOfRangeException(
                nameof(server.PackageMode),
                server.PackageMode,
                "PackageMode phải là 1, 2 hoặc 3.")
        };
    }

    private async Task<bool> ProcessMode1Async(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken)
    {
        List<StationMessage> messages =
            await GetPendingFtpMessagesAsync(
                server.KYHIEU_TRAM,
                ftp2,
                stoppingToken);

        if (messages.Count == 0)
            return false;

        bool processedAny = false;

        foreach (StationMessage message in messages)
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            bool allUploaded = true;

            foreach (SensorReading reading
                     in message.Measurements)
            {
                DateTime packageTimestamp =
                    DateTime.Now;

                string fileName =
                    BuildMode1FileName(
                        server,
                        reading.ParameterName,
                        packageTimestamp);

                string content =
                    BuildMode1Content(
                        message,
                        reading);

                byte[] data =
                    Encoding.UTF8.GetBytes(content);

                bool success =
                    await _ftpService.UploadBytesAsync(
                        server,
                        fileName,
                        data,
                        packageTimestamp,
                        stoppingToken);

                if (!success)
                {
                    allUploaded = false;

                    _logger.LogWarning(
                        "{FtpType} Mode 1 upload thất bại. " +
                        "MessageId={MessageId}, " +
                        "Station={Station}, " +
                        "Parameter={Parameter}",
                        GetFtpTypeName(ftp2),
                        message.Id,
                        message.StationName,
                        reading.ParameterName);

                    break;
                }
            }

            if (allUploaded)
            {
                await MarkFtpSentAsync(
                    new[] { message.Id },
                    ftp2,
                    stoppingToken);

                _lastSuccessfulProcess =
                    DateTime.Now;

                processedAny = true;

                _logger.LogInformation(
                    "{FtpType} Mode 1 hoàn thành. MessageId={MessageId}",
                    GetFtpTypeName(ftp2),
                    message.Id);
            }
        }

        return processedAny;
    }

    private async Task<bool> ProcessMode2Async(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken)
    {
        List<StationMessage> messages =
            await GetPendingFtpMessagesAsync(
                server.KYHIEU_TRAM,
                ftp2,
                stoppingToken);

        if (messages.Count == 0)
            return false;

        DateTime packageTimestamp =
            DateTime.Now;

        string fileName =
            BuildMode2FileName(
                server,
                packageTimestamp);

        string content =
            BuildMode2Content(messages);

        byte[] data =
            Encoding.UTF8.GetBytes(content);

        bool success =
            await _ftpService.UploadBytesAsync(
                server,
                fileName,
                data,
                packageTimestamp,
                stoppingToken);

        if (!success)
        {
            _logger.LogWarning(
                "{FtpType} Mode 2 upload thất bại. " +
                "Station={Station}, Messages={Count}",
                GetFtpTypeName(ftp2),
                server.KYHIEU_TRAM,
                messages.Count);

            return false;
        }

        await MarkFtpSentAsync(
            messages.Select(x => x.Id),
            ftp2,
            stoppingToken);

        _lastSuccessfulProcess =
            DateTime.Now;

        _logger.LogInformation(
            "{FtpType} Mode 2 hoàn thành. " +
            "Station={Station}, Messages={Count}",
            GetFtpTypeName(ftp2),
            server.KYHIEU_TRAM,
            messages.Count);

        return true;
    }

    private async Task<bool> ProcessMode3Async(
        FtpServerSettings server,
        bool ftp2,
        CancellationToken stoppingToken)
    {
        FtpSettings settings =
            ftp2
                ? _ftp2Settings
                : _ftpSettings;

        List<string> stationNames =
            settings.Servers
                .Where(x =>
                    string.Equals(
                        x.MA_TINH,
                        server.MA_TINH,
                        StringComparison.OrdinalIgnoreCase)
                    &&
                    string.Equals(
                        x.KYHIEU_CONGTRINH,
                        server.KYHIEU_CONGTRINH,
                        StringComparison.OrdinalIgnoreCase))
                .Select(x => x.KYHIEU_TRAM)
                .Where(x =>
                    !string.IsNullOrWhiteSpace(x))
                .Distinct(
                    StringComparer.OrdinalIgnoreCase)
                .ToList();

        if (stationNames.Count == 0)
            return false;

        List<StationMessage> messages =
            await GetPendingFtpMessagesAsync(
                stationNames,
                ftp2,
                stoppingToken);

        if (messages.Count == 0)
            return false;

        DateTime packageTimestamp =
            DateTime.Now;

        string fileName =
            BuildMode3FileName(
                server,
                packageTimestamp);

        string content =
            BuildMode3Content(messages);

        byte[] data =
            Encoding.UTF8.GetBytes(content);

        bool success =
            await _ftpService.UploadBytesAsync(
                server,
                fileName,
                data,
                packageTimestamp,
                stoppingToken);

        if (!success)
        {
            _logger.LogWarning(
                "{FtpType} Mode 3 upload thất bại. " +
                "CongTrinh={CongTrinh}, Messages={Count}",
                GetFtpTypeName(ftp2),
                server.KYHIEU_CONGTRINH,
                messages.Count);

            return false;
        }

        await MarkFtpSentAsync(
            messages.Select(x => x.Id),
            ftp2,
            stoppingToken);

        _lastSuccessfulProcess =
            DateTime.Now;

        _logger.LogInformation(
            "{FtpType} Mode 3 hoàn thành. " +
            "CongTrinh={CongTrinh}, Messages={Count}",
            GetFtpTypeName(ftp2),
            server.KYHIEU_CONGTRINH,
            messages.Count);

        return true;
    }

    private async Task<List<StationMessage>>
        GetPendingFtpMessagesAsync(
            string stationName,
            bool ftp2,
            CancellationToken stoppingToken)
    {
        return await GetPendingFtpMessagesAsync(
            new[] { stationName },
            ftp2,
            stoppingToken);
    }

    private async Task<List<StationMessage>>
        GetPendingFtpMessagesAsync(
            IEnumerable<string> stationNames,
            bool ftp2,
            CancellationToken stoppingToken)
    {
        if (ftp2)
        {
            return await _databaseApi
                .GetPendingFtp2MessagesAsync(
                    stationNames,
                    MaxMessagesPerFile,
                    stoppingToken);
        }

        return await _databaseApi
            .GetPendingFtpMessagesAsync(
                stationNames,
                MaxMessagesPerFile,
                stoppingToken);
    }

    private async Task MarkFtpSentAsync(
        IEnumerable<long> messageIds,
        bool ftp2,
        CancellationToken stoppingToken)
    {
        if (ftp2)
        {
            await _databaseApi.MarkFtp2SentAsync(
                messageIds,
                stoppingToken);

            return;
        }

        await _databaseApi.MarkFtpSentAsync(
            messageIds,
            stoppingToken);
    }

    private static string GetFtpTypeName(bool ftp2)
    {
        return ftp2
            ? "FTP2"
            : "FTP1";
    }

    private static string BuildMode1FileName(
        FtpServerSettings server,
        string parameterName,
        DateTime packageTimestamp)
    {
        string sendTime =
            packageTimestamp.ToString(
                "yyyyMMddHHmmss");

        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{server.KYHIEU_TRAM}_"
            + $"{parameterName}_"
            + $"{sendTime}.txt";
    }

    private static string BuildMode2FileName(
        FtpServerSettings server,
        DateTime packageTimestamp)
    {
        string sendTime =
            packageTimestamp.ToString(
                "yyyyMMddHHmmss");

        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{server.KYHIEU_TRAM}_"
            + $"{sendTime}.txt";
    }

    private static string BuildMode3FileName(
        FtpServerSettings server,
        DateTime packageTimestamp)
    {
        string sendTime =
            packageTimestamp.ToString(
                "yyyyMMddHHmmss");

        return $"{server.MA_TINH}_"
            + $"{server.KYHIEU_CONGTRINH}_"
            + $"{sendTime}.txt";
    }

    private static string BuildMode1Content(
        StationMessage message,
        SensorReading reading)
    {
        return $"{message.Timestamp}\t"
            + $"{reading.Value}\t"
            + $"{reading.Unit}\t"
            + $"{reading.Status:D2}"
            + Environment.NewLine;
    }

    private static string BuildMode2Content(
        IEnumerable<StationMessage> messages)
    {
        var builder =
            new StringBuilder();

        foreach (StationMessage message in messages)
        {
            foreach (SensorReading reading
                     in message.Measurements)
            {
                builder.AppendLine(
                    $"{reading.ParameterName}\t"
                    + $"{reading.Value}\t"
                    + $"{reading.Unit}\t"
                    + $"{message.Timestamp}\t"
                    + $"{reading.Status:D2}");
            }
        }

        return builder.ToString();
    }

    private static string BuildMode3Content(
        IEnumerable<StationMessage> messages)
    {
        var builder =
            new StringBuilder();

        foreach (StationMessage message in messages)
        {
            foreach (SensorReading reading
                     in message.Measurements)
            {
                builder.AppendLine(
                    $"{message.StationName}\t"
                    + $"{reading.ParameterName}\t"
                    + $"{reading.Value}\t"
                    + $"{reading.Unit}\t"
                    + $"{message.Timestamp}\t"
                    + $"{reading.Status:D2}");
            }
        }

        return builder.ToString();
    }

    private void ValidateServers(
        FtpSettings settings,
        string ftpType)
    {
        foreach (FtpServerSettings server
                 in settings.Servers)
        {
            if (string.IsNullOrWhiteSpace(
                    server.MA_TINH))
            {
                throw new ArgumentException(
                    $"{ftpType} MA_TINH không được rỗng.");
            }

            if (string.IsNullOrWhiteSpace(
                    server.KYHIEU_CONGTRINH))
            {
                throw new ArgumentException(
                    $"{ftpType} KYHIEU_CONGTRINH không được rỗng.");
            }

            if (string.IsNullOrWhiteSpace(
                    server.KYHIEU_TRAM))
            {
                throw new ArgumentException(
                    $"{ftpType} KYHIEU_TRAM không được rỗng.");
            }

            if (string.IsNullOrWhiteSpace(
                    server.IP))
            {
                throw new ArgumentException(
                    $"{ftpType} IP không được rỗng.");
            }

            if (server.Port <= 0 ||
                server.Port > 65535)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(server.Port),
                    server.Port,
                    $"{ftpType} Port không hợp lệ.");
            }

            if (server.PackageMode < 1 ||
                server.PackageMode > 3)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(server.PackageMode),
                    server.PackageMode,
                    $"{ftpType} PackageMode phải là 1, 2 hoặc 3.");
            }

            if (server.ModePath < 0 ||
                server.ModePath > 2)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(server.ModePath),
                    server.ModePath,
                    $"{ftpType} ModePath phải là 0, 1 hoặc 2.");
            }
        }
    }

    // ============================================================
    // HEALTH
    // ============================================================

    public object GetHealthStatus()
    {
        if (!_workerRunning)
        {
            return new
            {
                status = "Unhealthy",
                service = "FTP",
                worker = "Stopped"
            };
        }

        return new
        {
            status = "Healthy",
            service = "FTP",
            worker = "Running"
        };
    }

    // ============================================================
    // STATUS
    // ============================================================

    public object GetStatus()
    {
        return new
        {
            service = "FTP",

            worker = _workerRunning
                ? "Running"
                : "Stopped",

            ftp1 = new
            {
                enabled = _ftpSettings.Enabled,
                servers = _ftpSettings.Servers.Count
            },

            ftp2 = new
            {
                enabled = _ftp2Settings.Enabled,
                servers = _ftp2Settings.Servers.Count
            },

            lastSuccessfulProcess =
                _lastSuccessfulProcess,

            lastErrorTime =
                _lastErrorTime,

            lastError =
                _lastError
        };
    }
}