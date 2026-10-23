using FluentFTP;
using Ftp.Configuration;

namespace Ftp.Services;

public class FtpService
{
    private readonly ILogger<FtpService> _logger;

    public FtpService(ILogger<FtpService> logger)
    {
        _logger = logger;
    }

    public async Task<bool> UploadBytesAsync(
        FtpServerSettings server,
        string remoteFileName,
        byte[] data,
        DateTime packageTimestamp,
        CancellationToken cancellationToken
    )
    {
        if (server is null)
            throw new ArgumentNullException(nameof(server));

        if (string.IsNullOrWhiteSpace(server.IP))
            throw new ArgumentException("FTP IP không được rỗng.");

        if (string.IsNullOrWhiteSpace(server.User))
            throw new ArgumentException("FTP User không được rỗng.");

        if (string.IsNullOrWhiteSpace(remoteFileName))
            throw new ArgumentException("FTP filename không được rỗng.");

        string remotePath = BuildRemotePath(
            server.Path,
            remoteFileName,
            server.ModePath,
            packageTimestamp
        );

        using var client = new AsyncFtpClient(server.IP, server.User, server.Pass, server.Port);

        // FluentFTP timeout
        client.Config.ConnectTimeout = 10000;
        client.Config.ReadTimeout = 10000;
        client.Config.DataConnectionConnectTimeout = 10000;

        // Timeout tổng cho một lần FTP
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        timeoutCts.CancelAfter(TimeSpan.FromSeconds(60));

        try
        {
            // =====================================================
            // CONNECT
            // =====================================================

            await client.Connect(timeoutCts.Token);

            // =====================================================
            // UPLOAD
            // =====================================================

            FtpStatus status = await client.UploadBytes(
                data,
                remotePath,
                FtpRemoteExists.Overwrite,
                true,
                token: timeoutCts.Token
            );

            // =====================================================
            // UPLOAD XONG -> DISCONNECT
            // =====================================================

            await DisconnectSafelyAsync(client, cancellationToken);

            if (status == FtpStatus.Success)
            {
                _logger.LogInformation(
                    "FTP upload thành công. Server={Server}, File={File}",
                    server.IP,
                    remotePath
                );

                return true;
            }

            _logger.LogWarning(
                "FTP upload thất bại. Server={Server}, File={File}, Status={Status}",
                server.IP,
                remotePath,
                status
            );

            return false;
        }
        catch (OperationCanceledException)
            when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // =====================================================
            // TIMEOUT 60 GIÂY
            // =====================================================

            _logger.LogError(
                "FTP upload timeout sau 60 giây. " + "Server={Server}, File={File}",
                server.IP,
                remotePath
            );

            // Timeout rồi -> cố disconnect
            await DisconnectSafelyAsync(client, cancellationToken);

            return false;
        }
        catch (OperationCanceledException)
        {
            // =====================================================
            // WORKER / APPLICATION ĐANG SHUTDOWN
            // =====================================================

            _logger.LogInformation(
                "FTP upload bị hủy do ứng dụng dừng. " + "Server={Server}, File={File}",
                server.IP,
                remotePath
            );

            throw;
        }
        catch (Exception ex)
        {
            // =====================================================
            // FTP ERROR
            // =====================================================

            _logger.LogError(
                ex,
                "FTP upload lỗi. Server={Server}, File={File}",
                server.IP,
                remotePath
            );

            // Có lỗi -> cố disconnect
            await DisconnectSafelyAsync(client, cancellationToken);

            return false;
        }
    }

    private async Task DisconnectSafelyAsync(
        AsyncFtpClient client,
        CancellationToken cancellationToken
    )
    {
        try
        {
            if (!client.IsConnected)
                return;

            // Disconnect tối đa 5 giây.
            using var disconnectCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken
            );

            disconnectCts.CancelAfter(TimeSpan.FromSeconds(5));

            await client.Disconnect(disconnectCts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("FTP disconnect bị timeout hoặc bị hủy.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "FTP disconnect lỗi.");
        }
    }

    private static string BuildRemotePath(
        string directory,
        string fileName,
        int modePath,
        DateTime packageTimestamp
    )
    {
        string baseDirectory = directory?.TrimEnd('/') ?? "";

        string remoteDirectory = modePath switch
        {
            0 => baseDirectory,

            1 => $"{baseDirectory}/{packageTimestamp:yyyyMMdd}",

            2 => $"{baseDirectory}/{packageTimestamp:yyyy/MM/dd}",

            _ => throw new ArgumentOutOfRangeException(
                nameof(modePath),
                modePath,
                "ModePath phải là 0, 1 hoặc 2."
            ),
        };

        if (string.IsNullOrWhiteSpace(remoteDirectory))
            return "/" + fileName;

        return remoteDirectory.TrimEnd('/') + "/" + fileName;
    }
}
