namespace Database.Models;

public class SensorReading
{
    public string StationName { get; set; } = "";

    public byte SlaveId { get; set; }

    public string ParameterName { get; set; } = "";

    public double Value { get; set; }

    public string Unit { get; set; } = "";

    public int Status { get; set; }

    public string Timestamp { get; set; } = "";
}