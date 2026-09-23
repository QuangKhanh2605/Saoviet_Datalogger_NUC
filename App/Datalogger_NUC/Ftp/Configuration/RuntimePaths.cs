namespace Ftp.Configuration;

public static class RuntimePaths
{
    public static string Root =>
        Environment.GetEnvironmentVariable(
            "DATALOGGER_DATA_PATH")
        ?? Path.Combine(
            Directory.GetCurrentDirectory(),
            "RuntimeData");

    public static string AppSettings =>
        Path.Combine(
            Root,
            "FtpConfig.json");
}