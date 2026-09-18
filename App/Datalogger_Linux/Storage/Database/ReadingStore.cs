using Datalogger_Linux.Models;

namespace Datalogger_Linux.Services;

public class ReadingStore
{
    // =========================================================
    // LATEST READINGS
    // =========================================================

    private readonly object _readingLock = new();

    private readonly List<SensorReading> _latestReadings = new();

    // =========================================================
    // PARAMETER ERROR COUNTS
    // =========================================================
    //
    // Key:
    //
    // Station|Slave|Parameter
    //
    // Ví dụ:
    //
    // Station_01|1|Temperature
    // Station_01|1|pH
    // Station_01|1|EC
    //
    // Mỗi parameter có counter RIÊNG.
    //
    // =========================================================

    private readonly object _errorLock = new();

    private readonly Dictionary<string, int> _parameterErrorCounts = new(
        StringComparer.OrdinalIgnoreCase
    );

    // =========================================================
    // UPDATE READINGS
    // =========================================================

    public void UpdateReadings(List<SensorReading> readings)
    {
        if (readings == null || readings.Count == 0)
        {
            return;
        }

        lock (_readingLock)
        {
            foreach (SensorReading reading in readings)
            {
                UpdateReadingInternal(reading);
            }
        }
    }

    // =========================================================
    // UPDATE ONE READING
    // =========================================================

    public void UpdateReading(SensorReading reading)
    {
        if (reading == null)
        {
            return;
        }

        lock (_readingLock)
        {
            UpdateReadingInternal(reading);
        }
    }

    private void UpdateReadingInternal(SensorReading reading)
    {
        _latestReadings.RemoveAll(old =>
            old.StationName.Equals(reading.StationName, StringComparison.OrdinalIgnoreCase)
            && old.SlaveId == reading.SlaveId
            && old.ParameterName.Equals(reading.ParameterName, StringComparison.OrdinalIgnoreCase)
        );

        _latestReadings.Add(CloneReading(reading));
    }

    // =========================================================
    // GET CURRENT READINGS
    // =========================================================

    public List<SensorReading> GetLatestReadings()
    {
        lock (_readingLock)
        {
            return _latestReadings.Select(CloneReading).ToList();
        }
    }

    // =========================================================
    // GET ONE CURRENT READING
    // =========================================================

    public SensorReading? GetLatestReading(string stationName, byte slaveId, string parameterName)
    {
        lock (_readingLock)
        {
            SensorReading? reading = _latestReadings.FirstOrDefault(x =>
                x.StationName.Equals(stationName, StringComparison.OrdinalIgnoreCase)
                && x.SlaveId == slaveId
                && x.ParameterName.Equals(parameterName, StringComparison.OrdinalIgnoreCase)
            );

            if (reading == null)
            {
                return null;
            }

            return CloneReading(reading);
        }
    }

    // =========================================================
    // PARAMETER ERROR KEY
    // =========================================================

    private static string BuildParameterErrorKey(
        string stationName,
        byte slaveId,
        string parameterName
    )
    {
        return $"{stationName}|{slaveId}|{parameterName}";
    }

    // =========================================================
    // INCREMENT PARAMETER ERROR
    // =========================================================

    public int IncrementParameterErrorCount(string stationName, byte slaveId, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(stationName))
        {
            throw new ArgumentException("StationName không được rỗng.", nameof(stationName));
        }

        if (string.IsNullOrWhiteSpace(parameterName))
        {
            throw new ArgumentException("ParameterName không được rỗng.", nameof(parameterName));
        }

        string key = BuildParameterErrorKey(stationName, slaveId, parameterName);

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

    // =========================================================
    // GET PARAMETER ERROR COUNT
    // =========================================================

    public int GetParameterErrorCount(string stationName, byte slaveId, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(stationName) || string.IsNullOrWhiteSpace(parameterName))
        {
            return 0;
        }

        string key = BuildParameterErrorKey(stationName, slaveId, parameterName);

        lock (_errorLock)
        {
            if (_parameterErrorCounts.TryGetValue(key, out int count))
            {
                return count;
            }

            return 0;
        }
    }

    // =========================================================
    // RESET PARAMETER ERROR
    // =========================================================
    //
    // CHỈ gọi khi parameter ĐỌC THÀNH CÔNG.
    //
    // Không gọi khi:
    //
    // - Open COM thành công
    // - Reconnect
    // - Close COM
    // - Parameter đọc lỗi
    //
    // =========================================================

    public void ResetParameterErrorCount(string stationName, byte slaveId, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(stationName) || string.IsNullOrWhiteSpace(parameterName))
        {
            return;
        }

        string key = BuildParameterErrorKey(stationName, slaveId, parameterName);

        lock (_errorLock)
        {
            _parameterErrorCounts[key] = 0;
        }
    }

    // =========================================================
    // CLONE READING
    // =========================================================

    private static SensorReading CloneReading(SensorReading reading)
    {
        return new SensorReading
        {
            StationName = reading.StationName,

            SlaveId = reading.SlaveId,

            ParameterName = reading.ParameterName,

            Value = reading.Value,

            Unit = reading.Unit,

            Status = reading.Status,

            Timestamp = reading.Timestamp,
        };
    }
}
