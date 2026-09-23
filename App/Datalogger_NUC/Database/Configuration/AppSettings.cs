namespace Database.Configuration;

public class AppSettings
{
    public WebSettings Web { get; set; } = new();

    public ModbusApiSettings ModbusApi { get; set; } = new();

    public DatabaseSettings Database { get; set; } = new();
}

public class WebSettings
{
    public int Port { get; set; } = 5098;
}

public class ModbusApiSettings
{
    public string BaseUrl { get; set; } = "http://localhost:5097";
}

public class DatabaseSettings
{
    public int SaveIntervalMinutes { get; set; } = 5;

    public int SaveSecond { get; set; } = 0;

    public int RetentionDays { get; set; } = 30;

    public int MaxDatabaseSizeMB { get; set; } = 1024;

    public int CleanupIntervalSeconds { get; set; } = 3600;

    public bool VacuumAfterCleanup { get; set; } = true;
}
