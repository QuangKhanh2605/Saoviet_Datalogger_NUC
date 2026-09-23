namespace Ftp.Models;

public class SensorReading
{
    public string Name { get; set; } = "";

    public double Value { get; set; }

    public string Unit { get; set; } = "";

    public int Status { get; set; }
}
