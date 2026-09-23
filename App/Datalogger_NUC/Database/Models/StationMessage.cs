namespace Database.Models;

public class StationMessage
{
    public long Id { get; set; }

    public string Timestamp { get; set; } = "";

    public string StationName { get; set; } = "";

    public bool MqttSent { get; set; }

    public bool FtpSent { get; set; }

    public bool Ftp2Sent { get; set; }

    public List<SensorReading> Measurements { get; set; } = new();
}