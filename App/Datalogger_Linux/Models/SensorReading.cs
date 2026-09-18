namespace Datalogger_Linux.Models;

public class SensorReading
{
    // STATION
    public string StationName { get; set; } = "";

    // SLAVE
    public byte SlaveId { get; set; }

    // MEASUREMENT
    public string ParameterName { get; set; } = "";

    public double Value { get; set; }

    public string Unit { get; set; } = "";

    public int Status { get; set; }

    // TIMESTAMP
    public string Timestamp { get; set; } = "";
}
