namespace Ftp.Models;

public class StationMessage
{
    public long Id { get; set; }

    public string StationName { get; set; } = "";

    public string Timestamp { get; set; } = "";

    public List<SensorReading> Measurements { get; set; } = new();
}
