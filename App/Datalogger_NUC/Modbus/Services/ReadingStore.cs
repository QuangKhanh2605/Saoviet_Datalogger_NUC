using Modbus.Models;

namespace Modbus.Services;

public class ReadingStore
{
    private readonly object _readingLock = new();

    private readonly List<SensorReading> _latestReadings = [];

    private readonly object _errorLock = new();

    private readonly Dictionary<string, int> _parameterErrorCounts = new(
        StringComparer.OrdinalIgnoreCase
    );

    public void UpdateReadings(List<SensorReading> readings)
    {
        lock (_readingLock)
        {
            foreach (SensorReading reading in readings)
            {
                UpdateReadingInternal(reading);
            }
        }
    }

    public void UpdateReading(SensorReading reading)
    {
        lock (_readingLock)
        {
            UpdateReadingInternal(reading);
        }
    }

    private void UpdateReadingInternal(SensorReading reading)
    {
        int index = _latestReadings.FindIndex(x =>
            string.Equals(x.StationName, reading.StationName, StringComparison.OrdinalIgnoreCase)
            && x.SlaveId == reading.SlaveId
            && string.Equals(x.Name, reading.Name, StringComparison.OrdinalIgnoreCase)
        );

        if (index >= 0)
        {
            _latestReadings[index] = CloneReading(reading);
        }
        else
        {
            _latestReadings.Add(CloneReading(reading));
        }
    }

    public List<SensorReading> GetLatestReadings()
    {
        lock (_readingLock)
        {
            return _latestReadings.Select(CloneReading).ToList();
        }
    }

    public SensorReading? GetLatestReading(string stationName, byte slaveId, string Name)
    {
        lock (_readingLock)
        {
            SensorReading? reading = _latestReadings.FirstOrDefault(x =>
                string.Equals(x.StationName, stationName, StringComparison.OrdinalIgnoreCase)
                && x.SlaveId == slaveId
                && string.Equals(x.Name, Name, StringComparison.OrdinalIgnoreCase)
            );

            return reading == null ? null : CloneReading(reading);
        }
    }

    public int IncrementParameterErrorCount(string stationName, byte slaveId, string Name)
    {
        string key = BuildParameterKey(stationName, slaveId, Name);

        lock (_errorLock)
        {
            if (!_parameterErrorCounts.TryGetValue(key, out int count))
            {
                count = 0;
            }

            count++;

            _parameterErrorCounts[key] = count;

            return count;
        }
    }

    public int GetParameterErrorCount(string stationName, byte slaveId, string Name)
    {
        string key = BuildParameterKey(stationName, slaveId, Name);

        lock (_errorLock)
        {
            return _parameterErrorCounts.TryGetValue(key, out int count) ? count : 0;
        }
    }

    public void ResetParameterErrorCount(string stationName, byte slaveId, string Name)
    {
        string key = BuildParameterKey(stationName, slaveId, Name);

        lock (_errorLock)
        {
            _parameterErrorCounts.Remove(key);
        }
    }

    private static string BuildParameterKey(string stationName, byte slaveId, string Name)
    {
        return $"{stationName}|{slaveId}|{Name}";
    }

    private static SensorReading CloneReading(SensorReading reading)
    {
        return new SensorReading
        {
            StationName = reading.StationName,
            SlaveId = reading.SlaveId,
            Name = reading.Name,
            Value = reading.Value,
            Unit = reading.Unit,
            Status = reading.Status,
            Timestamp = reading.Timestamp,
        };
    }
}
