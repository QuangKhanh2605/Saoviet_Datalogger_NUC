using System.IO.Ports;
using Microsoft.Extensions.Options;
using Modbus.Configuration;
using Modbus.Hardware;
using Modbus.Models;
using Modbus.Sensors;
using Modbus.Services;

namespace Modbus.Workers;

public class ModbusWorker : BackgroundService
{
    private const string ModbusTcpProtocol = "ModbusTcp";
    private const string ModbusRtuProtocol = "ModbusRtu";

    private const string ReadingTimestampFormat = "yyyyMMddHHmmss";

    private const int ReconnectDelaySeconds = 2;
    private const int MaxParameterErrors = 3;

    private readonly ILogger<ModbusWorker> _logger;
    private readonly StationReader _stationReader;
    private readonly StationConfigurationService _configurationService;
    private readonly ReadingStore _readingStore;

    // Chỉ giữ số giây cấu hình.
    private readonly int _readIntervalSeconds;

    public ModbusWorker(
        ILogger<ModbusWorker> logger,
        StationReader stationReader,
        StationConfigurationService configurationService,
        ReadingStore readingStore,
        IOptions<AppSettings> options
    )
    {
        _logger = logger;
        _stationReader = stationReader;
        _configurationService = configurationService;
        _readingStore = readingStore;

        int readIntervalSeconds = options.Value.Sensor.ReadIntervalSeconds;

        if (readIntervalSeconds <= 0)
        {
            throw new InvalidOperationException(
                $"Sensor.ReadIntervalSeconds phải > 0. "
                    + $"Giá trị hiện tại: {readIntervalSeconds}."
            );
        }

        _readIntervalSeconds = readIntervalSeconds;
    }

    // =========================================================
    // WORKER
    // =========================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        List<StationDefinition> stations;

        try
        {
            stations = _configurationService.LoadAll();

            _logger.LogInformation("Đã load {Count} station configuration.", stations.Count);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Không thể load station configuration.");

            return;
        }

        if (stations.Count == 0)
        {
            _logger.LogWarning("Không có station nào được Enable.");

            return;
        }

        List<IGrouping<string, StationDefinition>> physicalGroups = stations
            .GroupBy(
                station => GetConnectionInfo(station.Connection).PhysicalKey,
                StringComparer.OrdinalIgnoreCase
            )
            .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        _logger.LogInformation("Đã tạo {Count} physical connection worker.", physicalGroups.Count);

        List<Task> workers = new();

        foreach (IGrouping<string, StationDefinition> group in physicalGroups)
        {
            List<StationDefinition> physicalStations = group.ToList();

            _logger.LogInformation(
                "Physical connection {Connection}: " + "{Count} station configuration.",
                group.Key,
                physicalStations.Count
            );

            workers.Add(RunPhysicalConnectionWorker(physicalStations, stoppingToken));
        }

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException)
        {
            // Worker dừng bình thường.
        }

        _logger.LogInformation("ModbusWorker stopped.");
    }

    // =========================================================
    // PHYSICAL CONNECTION WORKER
    // =========================================================

    private async Task RunPhysicalConnectionWorker(
        List<StationDefinition> stations,
        CancellationToken stoppingToken
    )
    {
        if (stations.Count == 0)
        {
            return;
        }

        ConnectionInfo physicalInfo = GetConnectionInfo(stations[0].Connection);

        string physicalKey = physicalInfo.PhysicalKey;

        List<IGrouping<string, StationDefinition>> connectionGroups = stations
            .GroupBy(
                station => GetConnectionInfo(station.Connection).ConnectionKey,
                StringComparer.OrdinalIgnoreCase
            )
            .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        _logger.LogInformation(
            "Physical connection {PhysicalConnection} có " + "{Count} cấu hình connection.",
            physicalKey,
            connectionGroups.Count
        );

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (IGrouping<string, StationDefinition> group in connectionGroups)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    await RunConnectionCycle(group.ToList(), stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Worker dừng bình thường.
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "Physical connection worker {Connection} bị dừng.",
                physicalKey
            );
        }

        _logger.LogInformation("Physical connection worker stopped: {Connection}", physicalKey);
    }

    // =========================================================
    // CONNECTION CYCLE
    // =========================================================

    private async Task RunConnectionCycle(
        List<StationDefinition> stations,
        CancellationToken stoppingToken
    )
    {
        if (stations.Count == 0)
        {
            return;
        }

        ConnectionDefinition connection = stations[0].Connection;

        ConnectionInfo connectionInfo = GetConnectionInfo(connection);

        string connectionKey = connectionInfo.ConnectionKey;

        DateTime cycleStart = DateTime.UtcNow;

        IModbusClient? client = null;

        try
        {
            // -------------------------------------------------
            // CREATE
            // -------------------------------------------------

            client = TryCreateClient(connection, connectionKey);

            if (client == null)
            {
                HandleConnectionFailure(stations, connectionKey);

                if (AreAllParametersFailed(stations))
                {
                    await Reconnect(connectionKey, stoppingToken);
                }
                else
                {
                    await WaitForNextCycle(cycleStart, stoppingToken);
                }

                return;
            }

            // -------------------------------------------------
            // OPEN
            // -------------------------------------------------

            if (!TryOpenClient(client, connectionKey))
            {
                HandleConnectionFailure(stations, connectionKey);

                if (AreAllParametersFailed(stations))
                {
                    await Reconnect(connectionKey, stoppingToken);
                }
                else
                {
                    await WaitForNextCycle(cycleStart, stoppingToken);
                }

                return;
            }

            // -------------------------------------------------
            // READ
            // -------------------------------------------------

            CycleStatus status = ReadStations(stations, client, connectionKey, stoppingToken);

            // -------------------------------------------------
            // CHECK RECONNECT
            // -------------------------------------------------

            if (AreAllParametersFailed(stations))
            {
                _logger.LogError(
                    "Tất cả parameter trên connection {Connection} "
                        + "đều đạt {MaxErrors} lỗi liên tiếp. "
                        + "Restart connection.",
                    connectionKey,
                    MaxParameterErrors
                );

                await Reconnect(connectionKey, stoppingToken);

                return;
            }

            // -------------------------------------------------
            // LOG
            // -------------------------------------------------

            LogCycleResult(connectionKey, status);

            // -------------------------------------------------
            // WAIT NEXT READ
            // -------------------------------------------------

            await WaitForNextCycle(cycleStart, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Worker dừng bình thường.
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "Connection worker {Connection} bị lỗi ngoài dự kiến.",
                connectionKey
            );
        }
        finally
        {
            SafeClose(client, connectionKey);
        }
    }

    // =========================================================
    // CREATE CLIENT
    // =========================================================

    private IModbusClient? TryCreateClient(ConnectionDefinition connection, string connectionKey)
    {
        try
        {
            _logger.LogDebug("Tạo Modbus client cho {Connection}.", connectionKey);

            return CreateClient(connection);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Không thể tạo Modbus client cho {Connection}.", connectionKey);

            return null;
        }
    }

    // =========================================================
    // OPEN CLIENT
    // =========================================================

    private bool TryOpenClient(IModbusClient client, string connectionKey)
    {
        try
        {
            _logger.LogInformation("Mở connection {Connection}.", connectionKey);

            client.Open();

            _logger.LogInformation("Connection {Connection} đã mở.", connectionKey);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Không thể mở connection {Connection}.", connectionKey);

            return false;
        }
    }

    // =========================================================
    // READ ALL STATIONS
    // =========================================================

    private CycleStatus ReadStations(
        List<StationDefinition> stations,
        IModbusClient client,
        string connectionKey,
        CancellationToken stoppingToken
    )
    {
        bool anySuccess = false;
        bool anyParameterFailure = false;

        foreach (StationDefinition station in stations)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            try
            {
                LogStationReadStart(station, connectionKey);

                StationReader.StationReadResult readResult = _stationReader.ReadStation(
                    station.StationName,
                    station.Sensors,
                    client
                );

                // -------------------------------------------------
                // SUCCESS
                // -------------------------------------------------

                if (ProcessSuccessfulReadings(readResult.Readings))
                {
                    anySuccess = true;
                }

                // -------------------------------------------------
                // CONNECTION FAILURE
                // -------------------------------------------------

                StationReader.SlaveReadError? connectionError = readResult.Errors.FirstOrDefault(
                    error => IsConnectionFailure(error.Exception)
                );

                if (connectionError != null)
                {
                    LogConnectionFailure(station, connectionError);

                    // Connection failure:
                    // Tất cả parameter của connection +1.
                    HandleConnectionFailure(stations, connectionKey);

                    // Kết thúc cycle ngay.
                    return CycleStatus.ConnectionFailure;
                }

                // -------------------------------------------------
                // PARAMETER ERROR
                // -------------------------------------------------

                foreach (StationReader.SlaveReadError error in readResult.Errors)
                {
                    if (error.Parameters.Count == 0)
                    {
                        LogGeneralReadError(station, error);

                        continue;
                    }

                    anyParameterFailure = true;

                    ProcessParameterErrors(station, error);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi ngoài dự kiến khi đọc "
                        + "Station={Station} | "
                        + "Connection={Connection}",
                    station.StationName,
                    connectionKey
                );
            }
        }

        if (anySuccess)
        {
            return CycleStatus.Success;
        }

        if (anyParameterFailure)
        {
            return CycleStatus.ParameterFailure;
        }

        return CycleStatus.NoData;
    }

    // =========================================================
    // PROCESS SUCCESSFUL READINGS
    // =========================================================

    private bool ProcessSuccessfulReadings(List<SensorReading> readings)
    {
        bool anySuccess = false;

        foreach (SensorReading reading in readings)
        {
            // Đọc thành công
            // => reset counter parameter về 0.

            _readingStore.ResetParameterErrorCount(
                reading.StationName,
                reading.SlaveId,
                reading.Name
            );

            _readingStore.UpdateReading(reading);

            anySuccess = true;

            _logger.LogDebug(
                "Parameter đọc thành công | "
                    + "Station={Station} | "
                    + "Slave={Slave} | "
                    + "Parameter={Parameter} | "
                    + "Value={Value} | "
                    + "Status={Status}",
                reading.StationName,
                reading.SlaveId,
                reading.Name,
                reading.Value,
                reading.Status
            );
        }

        return anySuccess;
    }

    // =========================================================
    // PROCESS PARAMETER ERRORS
    // =========================================================

    private void ProcessParameterErrors(
        StationDefinition station,
        StationReader.SlaveReadError error
    )
    {
        foreach (ParameterDefinition parameter in error.Parameters)
        {
            HandleParameterReadError(
                station,
                error.SlaveId,
                parameter,
                error.Exception,
                error.Message
            );
        }
    }

    // =========================================================
    // HANDLE PARAMETER ERROR
    //
    // Mỗi parameter có counter riêng.
    //
    // Đọc lỗi:
    //     count++
    //
    // count < 3:
    //     giữ reading cũ
    //
    // count >= 3:
    //     Value  = 0
    //     Status = Error
    //
    // Đọc thành công:
    //     counter = 0
    // =========================================================

    private void HandleParameterReadError(
        StationDefinition station,
        byte slaveId,
        ParameterDefinition parameter,
        Exception ex,
        string message
    )
    {
        int errorCount = _readingStore.IncrementParameterErrorCount(
            station.StationName,
            slaveId,
            parameter.Name
        );

        // -----------------------------------------------------
        // CHƯA ĐỦ 3 LẦN
        // -----------------------------------------------------

        if (errorCount < MaxParameterErrors)
        {
            _logger.LogWarning(
                ex,
                "{Message} | "
                    + "Station={Station} | "
                    + "Slave={Slave} | "
                    + "Parameter={Parameter} | "
                    + "Error={Count}/{MaxErrors} | "
                    + "GIỮ READING CŨ",
                message,
                station.StationName,
                slaveId,
                parameter.Name,
                errorCount,
                MaxParameterErrors
            );

            return;
        }

        // -----------------------------------------------------
        // ĐỦ 3 LẦN
        // -----------------------------------------------------

        SensorReading errorReading = CreateErrorReading(station, slaveId, parameter);

        _readingStore.UpdateReading(errorReading);

        _logger.LogError(
            ex,
            "{Message} | "
                + "Station={Station} | "
                + "Slave={Slave} | "
                + "Parameter={Parameter} | "
                + "Error={Count}/{MaxErrors} | "
                + "Value={Value} | "
                + "Status={Status}",
            message,
            station.StationName,
            slaveId,
            parameter.Name,
            errorCount,
            MaxParameterErrors,
            errorReading.Value,
            errorReading.Status
        );
    }

    // =========================================================
    // CREATE ERROR READING
    // =========================================================

    private static SensorReading CreateErrorReading(
        StationDefinition station,
        byte slaveId,
        ParameterDefinition parameter
    )
    {
        return new SensorReading
        {
            StationName = station.StationName,
            SlaveId = slaveId,
            Name = parameter.Name,
            Value = 0,
            Unit = parameter.Unit,
            Status = StatusCode.Error,
            Timestamp = DateTime.Now.ToString(ReadingTimestampFormat),
        };
    }

    // =========================================================
    // HANDLE CONNECTION FAILURE
    //
    // Connection failure:
    //
    // TẤT CẢ parameter thuộc connection
    //     => errorCount +1
    //
    // Sau đó cycle kết thúc.
    // =========================================================

    private void HandleConnectionFailure(List<StationDefinition> stations, string connectionKey)
    {
        _logger.LogWarning(
            "Connection failure: {Connection}. " + "Tất cả parameter sẽ được cộng 1 lỗi.",
            connectionKey
        );

        foreach (ParameterContext context in EnumerateParameters(stations))
        {
            HandleParameterReadError(
                context.Station,
                context.SlaveId,
                context.Parameter,
                new IOException("Connection không đọc được."),
                $"Không đọc được parameter " + $"do connection {connectionKey}."
            );
        }
    }

    // =========================================================
    // CHECK ALL PARAMETERS
    // =========================================================

    private bool AreAllParametersFailed(List<StationDefinition> stations)
    {
        bool hasParameter = false;

        foreach (ParameterContext context in EnumerateParameters(stations))
        {
            hasParameter = true;

            int errorCount = _readingStore.GetParameterErrorCount(
                context.Station.StationName,
                context.SlaveId,
                context.Parameter.Name
            );

            _logger.LogDebug(
                "Parameter error counter | "
                    + "Station={Station} | "
                    + "Slave={Slave} | "
                    + "Parameter={Parameter} | "
                    + "Count={Count}/{MaxErrors}",
                context.Station.StationName,
                context.SlaveId,
                context.Parameter.Name,
                errorCount,
                MaxParameterErrors
            );

            if (errorCount < MaxParameterErrors)
            {
                return false;
            }
        }

        return hasParameter;
    }

    // =========================================================
    // ENUMERATE PARAMETERS
    // =========================================================

    private static IEnumerable<ParameterContext> EnumerateParameters(
        IEnumerable<StationDefinition> stations
    )
    {
        foreach (StationDefinition station in stations)
        {
            foreach (SlaveDefinition slave in station.Sensors)
            {
                foreach (RegisterBlockDefinition block in slave.Blocks)
                {
                    foreach (ParameterDefinition parameter in block.Parameters)
                    {
                        yield return new ParameterContext(station, slave.SlaveId, parameter);
                    }
                }
            }
        }
    }

    // =========================================================
    // LOG
    // =========================================================

    private void LogStationReadStart(StationDefinition station, string connectionKey)
    {
        _logger.LogDebug(
            "Đọc Station={Station} | Connection={Connection}",
            station.StationName,
            connectionKey
        );
    }

    private void LogConnectionFailure(StationDefinition station, StationReader.SlaveReadError error)
    {
        _logger.LogWarning(
            error.Exception,
            "{Message} | Station={Station} | " + "Slave={Slave} | Connection failure.",
            error.Message,
            station.StationName,
            error.SlaveId
        );
    }

    private void LogGeneralReadError(StationDefinition station, StationReader.SlaveReadError error)
    {
        _logger.LogWarning(
            error.Exception,
            "{Message} | Station={Station} | Slave={Slave}",
            error.Message,
            station.StationName,
            error.SlaveId
        );
    }

    // =========================================================
    // CHECK CONNECTION FAILURE
    // =========================================================

    private static bool IsConnectionFailure(Exception exception)
    {
        return exception is InvalidOperationException;
    }

    // =========================================================
    // LOG CYCLE RESULT
    // =========================================================

    private void LogCycleResult(string connectionKey, CycleStatus status)
    {
        switch (status)
        {
            case CycleStatus.Success:

                _logger.LogDebug("Connection {Connection} hoạt động bình thường.", connectionKey);

                break;

            case CycleStatus.ParameterFailure:

                _logger.LogWarning(
                    "Connection {Connection}: " + "cycle có parameter lỗi.",
                    connectionKey
                );

                break;

            case CycleStatus.NoData:

                _logger.LogDebug(
                    "Connection {Connection}: " + "cycle không có dữ liệu.",
                    connectionKey
                );

                break;

            case CycleStatus.ConnectionFailure:

                _logger.LogWarning(
                    "Connection {Connection}: " + "cycle bị connection failure.",
                    connectionKey
                );

                break;
        }
    }

    // =========================================================
    // WAIT NEXT READ
    //
    // Chu kỳ đọc tính từ lúc BẮT ĐẦU cycle.
    //
    // Ví dụ ReadIntervalSeconds = 5:
    //
    // Đọc mất 2s:
    //     chờ thêm 3s
    //
    // Đọc mất 5s:
    //     đọc tiếp ngay
    //
    // Đọc mất 8s:
    //     đọc tiếp ngay
    // =========================================================

    private async Task WaitForNextCycle(DateTime cycleStart, CancellationToken stoppingToken)
    {
        TimeSpan elapsed = DateTime.UtcNow - cycleStart;

        double remainingSeconds = _readIntervalSeconds - elapsed.TotalSeconds;

        if (remainingSeconds <= 0)
        {
            return;
        }

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(remainingSeconds), stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Worker dừng bình thường.
        }
    }

    // =========================================================
    // RECONNECT
    // =========================================================

    private async Task Reconnect(string connectionKey, CancellationToken stoppingToken)
    {
        _logger.LogWarning(
            "Restart connection {Connection} sau {Seconds}s.",
            connectionKey,
            ReconnectDelaySeconds
        );

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(ReconnectDelaySeconds), stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Worker dừng bình thường.
        }
    }

    // =========================================================
    // CONNECTION INFO
    // =========================================================

    private static ConnectionInfo GetConnectionInfo(ConnectionDefinition connection)
    {
        if (connection.Protocol.Equals(ModbusTcpProtocol, StringComparison.OrdinalIgnoreCase))
        {
            string key = $"tcp:{connection.IpAddress}:{connection.Port}";

            return new ConnectionInfo(key, key);
        }

        if (connection.Protocol.Equals(ModbusRtuProtocol, StringComparison.OrdinalIgnoreCase))
        {
            string physicalKey = $"rtu:{connection.PortName}";

            string connectionKey =
                $"rtu:"
                + $"{connection.PortName}:"
                + $"{connection.BaudRate}:"
                + $"{connection.Parity}:"
                + $"{connection.DataBits}:"
                + $"{connection.StopBits}";

            return new ConnectionInfo(physicalKey, connectionKey);
        }

        string unknownKey = $"unknown:{connection.Protocol}";

        return new ConnectionInfo(unknownKey, unknownKey);
    }

    // =========================================================
    // CREATE MODBUS CLIENT
    // =========================================================

    private IModbusClient CreateClient(ConnectionDefinition connection)
    {
        if (connection.Protocol.Equals(ModbusRtuProtocol, StringComparison.OrdinalIgnoreCase))
        {
            return CreateRtuClient(connection);
        }

        if (connection.Protocol.Equals(ModbusTcpProtocol, StringComparison.OrdinalIgnoreCase))
        {
            return CreateTcpClient(connection);
        }

        throw new NotSupportedException($"Protocol không hỗ trợ: {connection.Protocol}");
    }

    // =========================================================
    // CREATE RTU CLIENT
    // =========================================================

    private IModbusClient CreateRtuClient(ConnectionDefinition connection)
    {
        if (string.IsNullOrWhiteSpace(connection.PortName))
        {
            throw new InvalidOperationException("Modbus RTU chưa có PortName.");
        }

        if (string.IsNullOrWhiteSpace(connection.Parity))
        {
            throw new InvalidOperationException("Modbus RTU chưa có Parity.");
        }

        if (string.IsNullOrWhiteSpace(connection.StopBits))
        {
            throw new InvalidOperationException("Modbus RTU chưa có StopBits.");
        }

        if (!Enum.TryParse<Parity>(connection.Parity, true, out Parity parity))
        {
            throw new InvalidOperationException($"Parity không hợp lệ: {connection.Parity}");
        }

        if (!Enum.TryParse<StopBits>(connection.StopBits, true, out StopBits stopBits))
        {
            throw new InvalidOperationException($"StopBits không hợp lệ: {connection.StopBits}");
        }

        return new ModbusRtu(
            connection.PortName,
            connection.BaudRate,
            parity,
            connection.DataBits,
            stopBits
        );
    }

    // =========================================================
    // CREATE TCP CLIENT
    // =========================================================

    private IModbusClient CreateTcpClient(ConnectionDefinition connection)
    {
        if (string.IsNullOrWhiteSpace(connection.IpAddress))
        {
            throw new InvalidOperationException("Modbus TCP chưa có IpAddress.");
        }

        return new ModbusTcp(connection.IpAddress, connection.Port);
    }

    // =========================================================
    // SAFE CLOSE
    // =========================================================

    private void SafeClose(IModbusClient? client, string connectionKey)
    {
        if (client == null)
        {
            return;
        }

        try
        {
            client.Close();

            _logger.LogInformation("Connection {Connection} đã đóng.", connectionKey);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Không thể đóng connection {Connection}.", connectionKey);
        }
    }

    // =========================================================
    // TYPES
    // =========================================================

    private enum CycleStatus
    {
        Success,
        ParameterFailure,
        NoData,
        ConnectionFailure,
    }

    private readonly record struct ConnectionInfo(string PhysicalKey, string ConnectionKey);

    private readonly record struct ParameterContext(
        StationDefinition Station,
        byte SlaveId,
        ParameterDefinition Parameter
    );
}
