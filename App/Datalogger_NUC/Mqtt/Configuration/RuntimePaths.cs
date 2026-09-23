namespace Database.Configuration;

public static class RuntimePaths
{
    public static string Root =>
        Environment.GetEnvironmentVariable("DATALOGGER_DATA_PATH")
        ?? Path.Combine(Directory.GetCurrentDirectory(), "RuntimeData");

    public static string AppSettings => Path.Combine(Root, "MqttConfig.json");

    public static string Database => Path.Combine(Root, "Database");
    public static string DatabaseFile => Path.Combine(Database, "gateway.db");

    public static string Logs => Path.Combine(Root, "Logs");
}
