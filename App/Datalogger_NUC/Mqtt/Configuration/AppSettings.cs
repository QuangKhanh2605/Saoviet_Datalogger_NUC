namespace Mqtt.Configuration;

public class AppSettings
{
    public DeviceSettings Device { get; set; } = new();

    public SensorSettings Sensor { get; set; } = new();

    public DatabaseSettings Database { get; set; } = new();

    public MqttSettings Mqtt { get; set; } = new();

    public FtpSettings Ftp { get; set; } = new();

    public FtpSettings Ftp2 { get; set; } = new();
}

// ============================================================
// DEVICE
// ============================================================

public class DeviceSettings
{
    public string Id { get; set; } = "";
    public string Ver { get; set; } = "";
}

// ============================================================
// SENSOR
// ============================================================

public class SensorSettings
{
    public int ReadIntervalSeconds { get; set; } = 1;
}

// ============================================================
// DATABASE
// ============================================================

public class DatabaseSettings
{
    public int SaveIntervalMinutes { get; set; } = 5;

    public int SaveSecond { get; set; } = 0;

    public int RetentionDays { get; set; } = 30;

    public int MaxDatabaseSizeMB { get; set; } = 1024;

    public int CleanupIntervalSeconds { get; set; } = 3600;

    public bool VacuumAfterCleanup { get; set; } = true;
}

// ============================================================
// MQTT
// ============================================================

public class MqttSettings
{
    public bool Enabled { get; set; }

    public int KeepAliveSeconds { get; set; } = 30;

    public string Topic { get; set; } = "";

    public string Sub { get; set; } = "";

    public int PublishQos { get; set; } = 1;

    public int SubscribeQos { get; set; } = 1;

    public MqttServerSettings Main { get; set; } = new();

    public MqttServerSettings Backup { get; set; } = new();
}

public class MqttServerSettings
{
    public string IP { get; set; } = "";

    public int Port { get; set; }

    public string User { get; set; } = "";

    public string Pass { get; set; } = "";
}

// ============================================================
// FTP1
// ============================================================

public class FtpSettings
{
    public bool Enabled { get; set; }

    public List<FtpServerSettings> Servers { get; set; } = new();
}

// ============================================================
// FTP2
// ============================================================

public class FtpSettings2
{
    public bool Enabled { get; set; }

    public List<FtpServerSettings> Servers { get; set; } = new();
}

// ============================================================
// FTP SERVER
// ============================================================

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

    // ========================================================
    // MODE PATH
    //
    // 0 = Path/File
    // 1 = Path/yyyyMMdd/File
    // 2 = Path/yyyy/MM/dd/File
    // ========================================================

    public int ModePath { get; set; } = 0;
}
