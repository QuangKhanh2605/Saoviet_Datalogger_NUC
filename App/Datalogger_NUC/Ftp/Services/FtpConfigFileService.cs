using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace Ftp.Services;

public sealed class FtpConfigFileService
{
    private readonly string _filePath;
    private readonly IConfigurationRoot _configuration;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public FtpConfigFileService(IConfiguration configuration)
    {
        _configuration =
            configuration as IConfigurationRoot
            ?? throw new InvalidOperationException("IConfiguration phải là IConfigurationRoot.");

        _filePath = Path.Combine(AppContext.BaseDirectory, "RuntimeData", "FtpConfig.json");
    }

    public async Task<JsonElement> ReadAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (!File.Exists(_filePath))
            {
                throw new FileNotFoundException("Không tìm thấy file cấu hình FTP.", _filePath);
            }

            await using FileStream stream = new(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite
            );

            using JsonDocument document = await JsonDocument.ParseAsync(
                stream,
                cancellationToken: cancellationToken
            );

            return document.RootElement.Clone();
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task WriteAsync(JsonElement config, CancellationToken cancellationToken = default)
    {
        if (config.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("Nội dung cấu hình FTP phải là một JSON object.");
        }

        await _lock.WaitAsync(cancellationToken);

        string tempPath = _filePath + ".tmp";

        try
        {
            string? directory = Path.GetDirectoryName(_filePath);

            if (string.IsNullOrWhiteSpace(directory))
            {
                throw new InvalidOperationException("Đường dẫn file cấu hình FTP không hợp lệ.");
            }

            Directory.CreateDirectory(directory);

            await using (
                FileStream stream = new(tempPath, FileMode.Create, FileAccess.Write, FileShare.None)
            )
            {
                await JsonSerializer.SerializeAsync(stream, config, JsonOptions, cancellationToken);

                await stream.FlushAsync(cancellationToken);
            }

            File.Move(tempPath, _filePath, overwrite: true);

            // Nạp lại cấu hình đồng bộ trước khi yêu cầu worker restart.
            _configuration.Reload();
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }

            _lock.Release();
        }
    }
}
