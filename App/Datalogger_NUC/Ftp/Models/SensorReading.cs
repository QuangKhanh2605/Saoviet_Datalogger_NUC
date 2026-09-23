namespace Ftp.Models;

public class SensorReading
{
    public string ParameterName { get; set; } = "";

    public double Value { get; set; }

    public string Unit { get; set; } = "";

    public int Status { get; set; }
}