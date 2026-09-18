using Datalogger_Linux.Models;

namespace Datalogger_Linux.Services;

public class StationMessageService
{
    public List<StationMessage> BuildStationMessages(List<SensorReading> readings)
    {
        if (readings.Count == 0)
        {
            return new List<StationMessage>();
        }

        var stationGroups = readings.GroupBy(r => r.StationName, StringComparer.OrdinalIgnoreCase);

        List<StationMessage> messages = new();

        foreach (var group in stationGroups)
        {
            SensorReading? firstReading = group.FirstOrDefault();

            string timestamp = !string.IsNullOrWhiteSpace(firstReading?.Timestamp)
                ? firstReading.Timestamp
                : DateTime.Now.ToString("yyyyMMddHHmmss");

            messages.Add(
                new StationMessage
                {
                    Timestamp = timestamp,

                    StationName = group.Key,

                    MqttSent = false,

                    FtpSent = false,

                    Measurements = group.Select(CloneReading).ToList(),
                }
            );
        }

        return messages;
    }

    private static SensorReading CloneReading(SensorReading reading)
    {
        return new SensorReading
        {
            StationName = reading.StationName,

            ParameterName = reading.ParameterName,

            Value = reading.Value,

            Unit = reading.Unit,

            Status = reading.Status,

            Timestamp = reading.Timestamp,
        };
    }
}
