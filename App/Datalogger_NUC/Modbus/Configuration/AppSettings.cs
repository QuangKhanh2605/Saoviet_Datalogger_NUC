namespace Modbus.Configuration;

public class AppSettings
{
    public SensorSettings Sensor { get; set; } = new();
}

public class SensorSettings
{
    public int ReadIntervalSeconds { get; set; } = 1;
}
