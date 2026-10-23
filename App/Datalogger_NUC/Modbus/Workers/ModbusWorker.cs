using System.IO.Ports;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Modbus.Configuration;
using Modbus.Hardware;
using Modbus.Models;
using Modbus.Sensors;
using Modbus.Services;
using StackExchange.Redis;

namespace Modbus.Workers;

public class ModbusWorker : BackgroundService
{
    private const int MaxParameterErrors = 3;
    private const string TimestampFormat = "yyyyMMddHHmmss";
    private const string RedisKeyPrefix = "modbus:readings:";

    private readonly ILogger<ModbusWorker> _logger;
    private readonly StationReader _stationReader;
    private readonly StationConfigurationService _configurationService;
    private readonly ReadingStore _readingStore;
    private readonly IOptionsMonitor<AppSettings> _options;
    private readonly IConnectionMultiplexer _redis;

    private readonly SemaphoreSlim _reloadSignal = new(0, 1);

    public ModbusWorker(
        ILogger<ModbusWorker> logger,
        StationReader stationReader,
        StationConfigurationService configurationService,
        ReadingStore readingStore,
        IOptionsMonitor<AppSettings> options,
        IConnectionMultiplexer redis
    )
    {
        _logger = logger;
        _stationReader = stationReader;
        _configurationService = configurationService;
        _readingStore = readingStore;
        _options = options;
        _redis = redis;
    }

    // ============================================================
    // RELOAD
    // ============================================================

    public void RequestReload()
    {
        try
        {
            _reloadSignal.Release();
            _logger.LogInformation("Modbus reload requested.");
        }
        catch (SemaphoreFullException)
        {
            // Đã có yêu cầu reload đang chờ.
        }
        catch (ObjectDisposedException)
        {
            _logger.LogDebug("Reload signal đã được giải phóng.");
        }
    }

    private async Task WaitForReloadAsync(CancellationToken token)
    {
        await _reloadSignal.WaitAsync(token);

        while (_reloadSignal.Wait(0)) { }
    }

    // ============================================================
    // EXECUTE
    // ============================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ModbusWorker started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await RunWorkerIterationAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Bảo vệ vòng lặp chính: lỗi bất ngờ không làm
                    // BackgroundService kết thúc vĩnh viễn.
                    _logger.LogError(ex, "Lỗi ngoài dự kiến trong ModbusWorker. Sẽ thử lại.");
                }

                if (stoppingToken.IsCancellationRequested)
                    break;

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Worker đang dừng.
        }
        finally
        {
            _logger.LogInformation("ModbusWorker stopped.");
        }
    }

    private async Task RunWorkerIterationAsync(CancellationToken stoppingToken)
    {
        int interval = _options.CurrentValue.Sensor.ReadIntervalSeconds;

        if (interval <= 0)
        {
            _logger.LogWarning("Sensor.ReadIntervalSeconds không hợp lệ: {Interval}.", interval);

            await WaitForConfigurationAsync(stoppingToken);
            return;
        }

        List<StationDefinition> stations;

        try
        {
            stations = _configurationService.LoadAll().Where(x => x.Enabled).ToList();
        }
        catch (Exception ex)
            when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Không thể load station.json. Sẽ thử lại.");

            await WaitForConfigurationAsync(stoppingToken);
            return;
        }

        if (stations.Count == 0)
        {
            _logger.LogWarning("Không có station nào được Enable.");

            await WaitForConfigurationAsync(stoppingToken);
            return;
        }

        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        using var reloadCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        Task sessionTask = RunStationsAsync(stations, sessionCts.Token);

        Task reloadTask = WaitForReloadAsync(reloadCts.Token);

        try
        {
            Task completed = await Task.WhenAny(sessionTask, reloadTask);

            if (completed == reloadTask)
            {
                await reloadTask;

                _logger.LogInformation("Đang reload cấu hình Modbus.");
            }
            else
            {
                // Nếu session kết thúc do exception, exception được
                // bắt ở đây; vòng lặp ngoài sẽ khởi động phiên mới.
                await sessionTask;

                _logger.LogWarning("Phiên đọc Modbus kết thúc.");
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Lỗi trong phiên đọc Modbus. Sẽ khởi động lại phiên.");
        }
        finally
        {
            await sessionCts.CancelAsync();

            await ObserveTaskAsync(sessionTask, "Phiên đọc Modbus");

            await reloadCts.CancelAsync();

            await ObserveTaskAsync(reloadTask, "Tác vụ chờ reload");
        }
    }

    private async Task ObserveTaskAsync(Task task, string taskName)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
            // Tác vụ đã được yêu cầu dừng.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{TaskName} kết thúc với lỗi.", taskName);
        }
    }

    private async Task WaitForConfigurationAsync(CancellationToken token)
    {
        try
        {
            if (await _reloadSignal.WaitAsync(TimeSpan.FromSeconds(5), token))
            {
                while (_reloadSignal.Wait(0)) { }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Worker đang dừng.
        }
    }

    // ============================================================
    // GLOBAL READ CYCLE
    // ============================================================

    private async Task RunStationsAsync(List<StationDefinition> stations, CancellationToken token)
    {
        var connections = stations
            .SelectMany(station =>
                station.Connections.Select(connection => (Station: station, Connection: connection))
            )
            .ToList();

        if (connections.Count == 0)
        {
            _logger.LogWarning("Không có connection nào được cấu hình.");

            return;
        }

        var groups = connections.GroupBy(x => PhysicalKey(x.Connection.Connection)).ToList();

        _logger.LogInformation(
            "Load {Stations} station, {Connections} connection, {Physical} physical connection.",
            stations.Count,
            connections.Count,
            groups.Count
        );

        while (!token.IsCancellationRequested)
        {
            DateTime cycleStart = DateTime.UtcNow;

            // Bước 1: Đọc các physical connection song song.
            // Exception của một nhóm được xử lý độc lập.
            await Task.WhenAll(
                groups.Select(async group =>
                {
                    token.ThrowIfCancellationRequested();

                    try
                    {
                        await RunPhysicalConnection(group.ToList(), token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Lỗi physical connection {Connection}. Tiếp tục các nhóm khác.",
                            group.Key
                        );
                    }
                })
            );

            token.ThrowIfCancellationRequested();

            // Bước 2: Cập nhật snapshot Redis.
            // Hàm này tự bắt lỗi Redis nên không làm dừng
            // chu kỳ đọc Modbus khi Redis mất kết nối.
            await UpdateRedisReadingsAsync(stations, token);

            _logger.LogDebug("Đã hoàn thành chu kỳ đọc và cập nhật Redis.");

            // Bước 3: Đợi chu kỳ kế tiếp.
            int interval = _options.CurrentValue.Sensor.ReadIntervalSeconds;

            if (interval <= 0)
            {
                _logger.LogWarning(
                    "Sensor.ReadIntervalSeconds không hợp lệ: {Interval}.",
                    interval
                );

                await Task.Delay(TimeSpan.FromSeconds(5), token);

                continue;
            }

            double remainingSeconds = interval - (DateTime.UtcNow - cycleStart).TotalSeconds;

            if (remainingSeconds > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(remainingSeconds), token);
            }
        }
    }

    // ============================================================
    // PHYSICAL CONNECTION
    // ============================================================

    private async Task RunPhysicalConnection(
        List<(StationDefinition Station, StationConnectionDefinition Connection)> connections,
        CancellationToken token
    )
    {
        var groups = connections.GroupBy(x => ConnectionKey(x.Connection.Connection)).ToList();

        foreach (var group in groups)
        {
            token.ThrowIfCancellationRequested();

            try
            {
                await RunConnection(group.ToList(), token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi connection {Connection}. Tiếp tục connection khác.",
                    group.Key
                );
            }
        }
    }

    // ============================================================
    // CONNECTION
    // ============================================================

    private async Task RunConnection(
        List<(StationDefinition Station, StationConnectionDefinition Connection)> connections,
        CancellationToken token
    )
    {
        if (connections.Count == 0)
            return;

        var definition = connections[0].Connection.Connection;

        string key = ConnectionKey(definition);

        IModbusClient? client = null;

        try
        {
            token.ThrowIfCancellationRequested();

            client = CreateClient(definition);

            try
            {
                client.Open();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Không mở được connection {Connection}.", key);

                HandleConnectionFailure(connections, key);

                return;
            }

            foreach (var item in connections)
            {
                token.ThrowIfCancellationRequested();

                ReadStation(item, client, key);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Đang reload hoặc dừng worker.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Connection {Connection} bị lỗi.", key);
        }
        finally
        {
            try
            {
                client?.Close();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Không thể đóng connection {Connection}.", key);
            }
        }

        await Task.CompletedTask;
    }

    // ============================================================
    // READ STATION
    // Chỉ cập nhật ReadingStore, không ghi Redis tại đây.
    // ============================================================

    private void ReadStation(
        (StationDefinition Station, StationConnectionDefinition Connection) item,
        IModbusClient client,
        string connectionKey
    )
    {
        try
        {
            var result = _stationReader.ReadStation(
                item.Station.StationName,
                item.Connection.Sensors,
                client
            );

            foreach (
                var failed in result
                    .FailedParameters.GroupBy(x => new { x.SlaveId, Parameter = x.Parameter.Name })
                    .Select(group => group.First())
            )
            {
                HandleParameterError(
                    connectionKey,
                    item.Station,
                    failed.SlaveId,
                    failed.Parameter,
                    new IOException("Không đọc được Value block."),
                    "Value block failure"
                );
            }

            foreach (var reading in result.Readings)
            {
                _readingStore.ResetParameterErrorCount(
                    connectionKey,
                    reading.StationName,
                    reading.SlaveId,
                    reading.Name
                );

                _readingStore.UpdateReading(connectionKey, reading);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Lỗi đọc Station={Station} Connection={Connection}.",
                item.Station.StationName,
                connectionKey
            );
        }
    }

    // ============================================================
    // REDIS
    // Mỗi station có một Hash chứa snapshot readings mới nhất.
    // Redis lỗi không được phép làm dừng chu kỳ đọc Modbus.
    // ============================================================

    private async Task UpdateRedisReadingsAsync(
        List<StationDefinition> stations,
        CancellationToken token
    )
    {
        try
        {
            IDatabase db = _redis.GetDatabase();

            var latestReadings = _readingStore.GetLatestReadings();

            var enabledStationNames = stations
                .Select(x => x.StationName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var readingsByStation = latestReadings
                .Where(x => enabledStationNames.Contains(x.StationName))
                .GroupBy(x => x.StationName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    group => group.Key,
                    group => group.ToList(),
                    StringComparer.OrdinalIgnoreCase
                );

            foreach (var station in stations)
            {
                token.ThrowIfCancellationRequested();

                string redisKey = $"{RedisKeyPrefix}{station.StationName}";

                readingsByStation.TryGetValue(station.StationName, out var stationReadings);

                var entries = (stationReadings ?? new List<SensorReading>())
                    .Select(reading => new HashEntry(
                        reading.Name,
                        JsonSerializer.Serialize(reading)
                    ))
                    .ToArray();

                ITransaction transaction = db.CreateTransaction();

                _ = transaction.KeyDeleteAsync(redisKey);

                if (entries.Length > 0)
                {
                    _ = transaction.HashSetAsync(redisKey, entries);
                }

                bool committed = await transaction.ExecuteAsync();

                if (!committed)
                {
                    _logger.LogWarning(
                        "Redis transaction không thành công cho Station={Station}.",
                        station.StationName
                    );

                    continue;
                }

                _logger.LogDebug(
                    "Đã cập nhật Redis cho Station={Station}, Readings={Count}.",
                    station.StationName,
                    entries.Length
                );
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(
                ex,
                "Redis không khả dụng. Modbus vẫn tiếp tục đọc; sẽ thử ghi lại ở chu kỳ sau."
            );
        }
        catch (Exception ex)
        {
            // Bao gồm lỗi phát sinh khi tạo snapshot hoặc ghi Redis.
            // Không cho lỗi cập nhật snapshot làm dừng worker.
            _logger.LogWarning(
                ex,
                "Không thể cập nhật readings vào Redis. Modbus vẫn tiếp tục chạy."
            );
        }
    }

    // ============================================================
    // PARAMETER ERROR
    // ============================================================

    private void HandleParameterError(
        string connectionKey,
        StationDefinition station,
        byte slaveId,
        ParameterDefinition parameter,
        Exception ex,
        string message
    )
    {
        int count = _readingStore.IncrementParameterErrorCount(
            connectionKey,
            station.StationName,
            slaveId,
            parameter.Name
        );

        if (count < MaxParameterErrors)
        {
            _logger.LogWarning(
                ex,
                "{Message} | Station={Station} | Connection={Connection} | Slave={Slave} | Parameter={Parameter} | ErrorCount={Count}/{Max}",
                message,
                station.StationName,
                connectionKey,
                slaveId,
                parameter.Name,
                count,
                MaxParameterErrors
            );

            return;
        }

        var reading = new SensorReading
        {
            StationName = station.StationName,
            SlaveId = slaveId,
            Name = parameter.Name,
            Value = 0,
            Unit = parameter.Unit,
            Status = StatusCode.Error,
            Timestamp = DateTime.Now.ToString(TimestampFormat),
        };

        _readingStore.UpdateReading(connectionKey, reading);

        _logger.LogError(
            ex,
            "Parameter lỗi {Count}/{Max} | Station={Station} | Connection={Connection} | Slave={Slave} | Parameter={Parameter} | Value={Value} | Unit={Unit} | Status={Status}",
            count,
            MaxParameterErrors,
            station.StationName,
            connectionKey,
            slaveId,
            parameter.Name,
            reading.Value,
            reading.Unit,
            reading.Status
        );
    }

    private void HandleConnectionFailure(
        List<(StationDefinition Station, StationConnectionDefinition Connection)> connections,
        string connectionKey
    )
    {
        foreach (var item in connections)
        {
            foreach (var slave in item.Connection.Sensors)
            {
                foreach (var block in slave.Blocks)
                {
                    foreach (var parameter in block.Parameters)
                    {
                        HandleParameterError(
                            connectionKey,
                            item.Station,
                            slave.SlaveId,
                            parameter,
                            new IOException("Connection không đọc được."),
                            "Connection failure"
                        );
                    }
                }
            }
        }
    }

    // ============================================================
    // MODBUS CLIENT
    // ============================================================

    private static IModbusClient CreateClient(ConnectionDefinition connection)
    {
        if (connection.Protocol.Equals("ModbusRtu", StringComparison.OrdinalIgnoreCase))
        {
            if (!Enum.TryParse(connection.Parity, true, out Parity parity))
            {
                throw new InvalidOperationException($"Parity không hợp lệ: {connection.Parity}");
            }

            if (!Enum.TryParse(connection.StopBits, true, out StopBits stopBits))
            {
                throw new InvalidOperationException(
                    $"StopBits không hợp lệ: {connection.StopBits}"
                );
            }

            return new ModbusRtu(
                connection.PortName,
                connection.BaudRate,
                parity,
                connection.DataBits,
                stopBits
            );
        }

        if (connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase))
        {
            return new ModbusTcp(connection.IpAddress, connection.Port);
        }

        throw new NotSupportedException($"Protocol không hỗ trợ: {connection.Protocol}");
    }

    // ============================================================
    // CONNECTION KEYS
    // ============================================================

    private static string PhysicalKey(ConnectionDefinition connection)
    {
        if (connection.Protocol.Equals("ModbusRtu", StringComparison.OrdinalIgnoreCase))
        {
            return $"rtu:{connection.PortName}";
        }

        if (connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase))
        {
            return $"tcp:{connection.IpAddress}:{connection.Port}";
        }

        return $"unknown:{connection.Protocol}";
    }

    private static string ConnectionKey(ConnectionDefinition connection)
    {
        if (connection.Protocol.Equals("ModbusRtu", StringComparison.OrdinalIgnoreCase))
        {
            return $"rtu:{connection.PortName}:{connection.BaudRate}:"
                + $"{connection.Parity}:{connection.DataBits}:{connection.StopBits}";
        }

        if (connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase))
        {
            return $"tcp:{connection.IpAddress}:{connection.Port}";
        }

        return $"unknown:{connection.Protocol}";
    }
}
