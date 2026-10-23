using System.Text.Json;
using Database.Configuration;
using Microsoft.Extensions.Configuration;

namespace Database.Services;

public sealed class DatabaseConfigFileService
{
    private readonly string _configPath;
    private readonly IConfigurationRoot _configuration;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
    };

    public DatabaseConfigFileService(IConfigurationRoot configuration)
    {
        _configuration = configuration;

        _configPath = Path.GetFullPath(
            Path.Combine(AppContext.BaseDirectory, RuntimePaths.AppSettings)
        );
    }

    // =============================================================
    // READ CONFIGURATION FILE
    // =============================================================

    public string ReadConfig()
    {
        if (!File.Exists(_configPath))
        {
            throw new FileNotFoundException(
                "Database configuration file was not found.",
                _configPath
            );
        }

        string json = File.ReadAllText(_configPath);

        using JsonDocument document = JsonDocument.Parse(json);

        return JsonSerializer.Serialize(document.RootElement, JsonOptions);
    }

    // =============================================================
    // VALIDATE AND WRITE CONFIGURATION FILE
    // =============================================================

    public void WriteConfig(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            throw new ArgumentException("Configuration JSON cannot be empty.", nameof(json));
        }

        using JsonDocument document = JsonDocument.Parse(json);

        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("Configuration must be a JSON object.", nameof(json));
        }

        AppSettings? settings = JsonSerializer.Deserialize<AppSettings>(json, JsonOptions);

        if (settings == null)
        {
            throw new ArgumentException(
                "Could not deserialize database configuration.",
                nameof(json)
            );
        }

        ValidateSettings(settings);

        string formattedJson = JsonSerializer.Serialize(settings, JsonOptions);

        string? directory = Path.GetDirectoryName(_configPath);

        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        string tempPath = _configPath + ".tmp";

        try
        {
            File.WriteAllText(tempPath, formattedJson);

            // Replace the original file after the temporary file is written.
            File.Move(tempPath, _configPath, overwrite: true);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }

        // Reload IConfiguration so IOptionsMonitor receives the new values.
        _configuration.Reload();
    }

    // =============================================================
    // VALIDATE SETTINGS
    // =============================================================

    private static void ValidateSettings(AppSettings settings)
    {
        if (settings.Database.SaveIntervalMinutes <= 0)
        {
            throw new ArgumentException("Database.SaveIntervalMinutes must be greater than 0.");
        }

        if (settings.Database.SaveSecond < 0 || settings.Database.SaveSecond > 59)
        {
            throw new ArgumentException("Database.SaveSecond must be between 0 and 59.");
        }

        if (settings.Database.RetentionDays <= 0)
        {
            throw new ArgumentException("Database.RetentionDays must be greater than 0.");
        }

        if (settings.Database.MaxDatabaseSizeMB <= 0)
        {
            throw new ArgumentException("Database.MaxDatabaseSizeMB must be greater than 0.");
        }

        if (settings.Database.CleanupIntervalSeconds <= 0)
        {
            throw new ArgumentException("Database.CleanupIntervalSeconds must be greater than 0.");
        }

        if (
            string.IsNullOrWhiteSpace(settings.ModbusApi.BaseUrl)
            || !Uri.TryCreate(settings.ModbusApi.BaseUrl, UriKind.Absolute, out Uri? baseUri)
            || (baseUri.Scheme != Uri.UriSchemeHttp && baseUri.Scheme != Uri.UriSchemeHttps)
        )
        {
            throw new ArgumentException("ModbusApi.BaseUrl must be a valid HTTP or HTTPS URL.");
        }

        if (settings.Web.Port < 1 || settings.Web.Port > 65535)
        {
            throw new ArgumentException("Web.Port must be between 1 and 65535.");
        }
    }
}
