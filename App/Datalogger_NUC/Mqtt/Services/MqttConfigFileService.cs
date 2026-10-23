using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace Mqtt.Services;

public sealed class MqttConfigFileService
{
    private readonly string _filePath;
    private readonly IConfiguration _configuration;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public MqttConfigFileService(IConfiguration configuration)
    {
        _configuration = configuration;

        _filePath = Path.Combine(AppContext.BaseDirectory, "RuntimeData", "MqttConfig.json");
    }

    // =========================================================
    // READ
    // =========================================================

    public async Task<string> ReadAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (!File.Exists(_filePath))
            {
                throw new FileNotFoundException("Không tìm thấy MqttConfig.json.", _filePath);
            }

            return await File.ReadAllTextAsync(_filePath, cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    // =========================================================
    // WRITE AND RELOAD
    // =========================================================

    public async Task WriteAsync(JsonElement config, CancellationToken cancellationToken = default)
    {
        if (config.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("Cấu hình MQTT phải là một JSON object.");
        }

        // Kiểm tra JSON và định dạng lại.
        string json = JsonSerializer.Serialize(config, JsonOptions);

        using (JsonDocument.Parse(json)) { }

        await _lock.WaitAsync(cancellationToken);

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);

            string tempPath = _filePath + ".tmp";

            try
            {
                await File.WriteAllTextAsync(tempPath, json, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();

                // Thay thế file cấu hình chính.
                File.Move(tempPath, _filePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }

            // Nạp lại toàn bộ configuration providers.
            // Phải thực hiện sau khi file mới đã được ghi xong.
            if (_configuration is not IConfigurationRoot configurationRoot)
            {
                throw new InvalidOperationException(
                    "Không thể reload cấu hình: IConfiguration "
                        + "không triển khai IConfigurationRoot."
                );
            }

            configurationRoot.Reload();
        }
        finally
        {
            _lock.Release();
        }
    }
}
