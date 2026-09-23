namespace Ftp.Configuration;

public class AppSettings
{
    public DeviceSettings Device { get; set; } = new();

    public DatabaseApiSettings DatabaseApi { get; set; } = new();

    public FtpSettings Ftp { get; set; } = new();

    public FtpSettings Ftp2 { get; set; } = new();
}

public class DeviceSettings
{
    public string Id { get; set; } = "";
    public string Ver { get; set; } = "";
}

public class DatabaseApiSettings
{
    public string BaseUrl { get; set; } = "";
}

public class FtpSettings
{
    public bool Enabled { get; set; }

    public List<FtpServerSettings> Servers { get; set; } = new();
}

public class FtpServerSettings
{
    public string MA_TINH { get; set; } = "";

    public string KYHIEU_CONGTRINH { get; set; } = "";

    public string KYHIEU_TRAM { get; set; } = "";

    public string IP { get; set; } = "";

    public int Port { get; set; } = 21;

    public string User { get; set; } = "";

    public string Pass { get; set; } = "";

    public string Path { get; set; } = "";

    public int PackageMode { get; set; } = 1;

    public int ModePath { get; set; } = 0;
}
