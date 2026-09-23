namespace Database.Configuration;

public static class RuntimePaths
{
    /// <summary>
    /// Thư mục gốc chứa toàn bộ dữ liệu runtime.
    /// Có thể thay đổi bằng biến môi trường DATALOGGER_DATA_PATH.
    /// </summary>
    public static string Root =>
        Environment.GetEnvironmentVariable(
            "DATALOGGER_DATA_PATH")
        ?? Path.Combine(
            Directory.GetCurrentDirectory(),
            "RuntimeData");

    /// <summary>
    /// File cấu hình Database.
    /// </summary>
    public static string AppSettings =>
        Path.Combine(
            Root,
            "DatabaseConfig.json");

    /// <summary>
    /// Thư mục chứa SQLite Database.
    /// </summary>
    public static string Database =>
        Path.Combine(
            Root,
            "Database");

    /// <summary>
    /// File SQLite Database.
    /// </summary>
    public static string DatabaseFile =>
        Path.Combine(
            Database,
            "gateway.db");

    /// <summary>
    /// Thư mục log.
    /// </summary>
    public static string Logs =>
        Path.Combine(
            Root,
            "Logs");
}