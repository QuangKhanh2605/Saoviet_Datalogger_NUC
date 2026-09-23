namespace Modbus.Configuration;

public class AppSettings
{
    public DeviceSettings Device { get; set; } = new();

    public SensorSettings Sensor { get; set; } = new();
}

public class DeviceSettings
{
    public string Id { get; set; } = "";

    public string Ver { get; set; } = "";
}

public class SensorSettings
{
    public int ReadIntervalSeconds { get; set; } = 1;
}
