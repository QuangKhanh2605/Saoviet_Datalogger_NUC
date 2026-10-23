using Modbus.Models;

namespace Modbus.Services;

public class ReadingStore
{
    // ============================================================
    // LATEST READINGS
    // ============================================================

    private readonly object _readingLock = new();

    private readonly Dictionary<string, SensorReading> _latestReadings = new(
        StringComparer.OrdinalIgnoreCase
    );

    public void UpdateReading(string connectionKey, SensorReading reading)
    {
        lock (_readingLock)
        {
            string key = BuildKey(
                connectionKey,
                reading.StationName,
                reading.SlaveId,
                reading.Name
            );

            _latestReadings[key] = CloneReading(reading);
        }
    }

    public List<SensorReading> GetLatestReadings()
    {
        lock (_readingLock)
        {
            return _latestReadings.Values.Select(CloneReading).ToList();
        }
    }

    // ============================================================
    // PARAMETER ERROR COUNTERS
    // ============================================================

    private readonly object _errorLock = new();

    private readonly Dictionary<string, int> _parameterErrorCounts = new(
        StringComparer.OrdinalIgnoreCase
    );

    public int IncrementParameterErrorCount(
        string connectionKey,
        string stationName,
        byte slaveId,
        string parameterName
    )
    {
        string key = BuildKey(connectionKey, stationName, slaveId, parameterName);

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

    public void ResetParameterErrorCount(
        string connectionKey,
        string stationName,
        byte slaveId,
        string parameterName
    )
    {
        string key = BuildKey(connectionKey, stationName, slaveId, parameterName);

        lock (_errorLock)
        {
            _parameterErrorCounts.Remove(key);
        }
    }

    // ============================================================
    // KEY
    // ============================================================

    private static string BuildKey(
        string connectionKey,
        string stationName,
        byte slaveId,
        string parameterName
    )
    {
        return $"{connectionKey}|{stationName}|{slaveId}|{parameterName}";
    }

    // ============================================================
    // CLONE
    // ============================================================

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
