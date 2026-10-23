using System.IO.Ports;
using System.Text.Json;
using Modbus.Sensors;

namespace Modbus.Configuration;

public sealed class StationConfigurationService
{
    private readonly string _directoryPath;
    private readonly string _filePath;
    private readonly object _fileLock = new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public StationConfigurationService(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Đường dẫn station không được để trống.", nameof(path));

        if (path.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            _filePath = Path.GetFullPath(path);
            _directoryPath =
                Path.GetDirectoryName(_filePath)
                ?? throw new ArgumentException("Đường dẫn file không hợp lệ.", nameof(path));
        }
        else
        {
            _directoryPath = Path.GetFullPath(path);
            _filePath = Path.Combine(_directoryPath, "station.json");
        }
    }

    // ============================================================
    // READ RAW JSON
    // ============================================================

    public string ReadRaw()
    {
        lock (_fileLock)
        {
            if (!File.Exists(_filePath))
            {
                throw new FileNotFoundException("Không tìm thấy station.json.", _filePath);
            }

            return File.ReadAllText(_filePath);
        }
    }

    // ============================================================
    // SAVE RAW JSON
    // Chỉ ghi đè file gốc sau khi JSON và cấu hình đã hợp lệ.
    // ============================================================

    public void SaveRaw(string json)
    {
        ValidateRaw(json);

        lock (_fileLock)
        {
            Directory.CreateDirectory(_directoryPath);

            string tempPath = _filePath + "." + Guid.NewGuid().ToString("N") + ".tmp";

            try
            {
                File.WriteAllText(tempPath, json);

                File.Move(tempPath, _filePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
        }
    }

    public List<StationDefinition> ParseRaw(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);

        List<StationDefinition> stations;

        if (document.RootElement.ValueKind == JsonValueKind.Array)
        {
            stations =
                JsonSerializer.Deserialize<List<StationDefinition>>(json, JsonOptions) ?? new();
        }
        else if (document.RootElement.ValueKind == JsonValueKind.Object)
        {
            // Hỗ trợ format cũ có wrapper Stations.
            if (TryGetStationsElement(document.RootElement, out JsonElement stationsElement))
            {
                stations =
                    JsonSerializer.Deserialize<List<StationDefinition>>(
                        stationsElement.GetRawText(),
                        JsonOptions
                    ) ?? new();
            }
            else
            {
                StationDefinition? station = JsonSerializer.Deserialize<StationDefinition>(
                    json,
                    JsonOptions
                );

                stations = station is null ? new() : new() { station };
            }
        }
        else
        {
            throw new InvalidDataException(
                "station.json phải là object, array hoặc object chứa Stations."
            );
        }

        if (stations.Count == 0)
            throw new InvalidDataException("station.json không có station nào.");

        HashSet<string> stationNames = new(StringComparer.OrdinalIgnoreCase);

        foreach (StationDefinition? station in stations)
        {
            if (station is null)
                throw new InvalidDataException("station.json chứa station null.");

            if (string.IsNullOrWhiteSpace(station.StationName))
                throw new InvalidDataException("StationName chưa được cấu hình.");

            if (!stationNames.Add(station.StationName.Trim()))
            {
                throw new InvalidDataException($"StationName bị trùng: {station.StationName}.");
            }

            // Kiểm tra cả station disabled để không lưu cấu hình sai.
            ValidateStation(_filePath, station);
        }

        return stations;
    }

    public void ValidateRaw(string json)
    {
        _ = ParseRaw(json);
    }

    private static bool TryGetStationsElement(JsonElement root, out JsonElement stationsElement)
    {
        foreach (JsonProperty property in root.EnumerateObject())
        {
            if (property.Name.Equals("Stations", StringComparison.OrdinalIgnoreCase))
            {
                stationsElement = property.Value;

                if (stationsElement.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidDataException("Thuộc tính Stations phải là một array.");
                }

                return true;
            }
        }

        stationsElement = default;
        return false;
    }

    // ============================================================
    // LOAD ALL ENABLED STATIONS
    // ============================================================

    public List<StationDefinition> LoadAll()
    {
        string json = ReadRaw();
        List<StationDefinition> stations = ParseRaw(json);

        List<StationDefinition> enabledStations = stations
            .Where(station => station.Enabled)
            .ToList();

        foreach (StationDefinition station in stations)
        {
            if (!station.Enabled)
            {
                Console.WriteLine($"Bỏ qua station disabled: {station.StationName}");
            }
            else
            {
                Console.WriteLine($"Station OK: {station.StationName}");
            }
        }

        Console.WriteLine(
            $"Load station hoàn tất. "
                + $"ValidStations={enabledStations.Count} "
                + $"TotalStations={stations.Count}"
        );

        return enabledStations;
    }

    // ============================================================
    // VALIDATE STATION
    // ============================================================

    private static void ValidateStation(string file, StationDefinition station)
    {
        if (string.IsNullOrWhiteSpace(station.StationName))
            throw new InvalidDataException("StationName chưa được cấu hình.");

        if (station.Connections is null || station.Connections.Count == 0)
        {
            throw new InvalidDataException(
                $"Station '{station.StationName}' không có Connections."
            );
        }

        HashSet<string> connectionKeys = new(StringComparer.OrdinalIgnoreCase);

        foreach (StationConnectionDefinition? stationConnection in station.Connections)
        {
            if (stationConnection is null)
            {
                throw new InvalidDataException(
                    $"Station '{station.StationName}' có Connection null."
                );
            }

            ConnectionDefinition? connection = stationConnection.Connection;

            if (connection is null)
            {
                throw new InvalidDataException(
                    $"Station '{station.StationName}' chưa có Connection."
                );
            }

            ValidateConnection(file, station, connection);

            string connectionKey = GetConnectionKey(connection);

            if (!connectionKeys.Add(connectionKey))
            {
                throw new InvalidDataException(
                    $"Station '{station.StationName}' " + $"Connection '{connectionKey}' bị trùng."
                );
            }

            if (stationConnection.Sensors is null || stationConnection.Sensors.Count == 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' "
                        + $"Connection='{connectionKey}' không có Sensors."
                );
            }

            HashSet<byte> slaveIds = new();

            foreach (SlaveDefinition? slave in stationConnection.Sensors)
            {
                if (slave is null)
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' có Sensor null."
                    );
                }

                if (!slaveIds.Add(slave.SlaveId))
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' "
                            + $"Connection='{connectionKey}' "
                            + $"SlaveId={slave.SlaveId} bị trùng."
                    );
                }

                ValidateSlave(file, station, stationConnection, slave);
            }

            ValidateWrites(file, station, stationConnection);
        }
    }

    // ============================================================
    // VALIDATE CONNECTION
    // ============================================================

    private static void ValidateConnection(
        string file,
        StationDefinition station,
        ConnectionDefinition connection
    )
    {
        if (string.IsNullOrWhiteSpace(connection.Protocol))
        {
            throw new InvalidDataException($"Station='{station.StationName}' chưa có Protocol.");
        }

        bool rtu = connection.Protocol.Equals("ModbusRtu", StringComparison.OrdinalIgnoreCase);

        bool tcp = connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase);

        if (!rtu && !tcp)
        {
            throw new InvalidDataException(
                $"Station='{station.StationName}' "
                    + $"Protocol không hỗ trợ: {connection.Protocol}"
            );
        }

        if (rtu)
        {
            if (string.IsNullOrWhiteSpace(connection.PortName))
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' ModbusRtu chưa có PortName."
                );
            }

            if (connection.BaudRate <= 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' BaudRate phải > 0."
                );
            }

            if (connection.DataBits <= 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' DataBits phải > 0."
                );
            }

            if (
                !Enum.TryParse(connection.Parity, true, out Parity parity)
                || !Enum.IsDefined(parity)
            )
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Parity không hợp lệ: "
                        + $"{connection.Parity}"
                );
            }

            if (
                !Enum.TryParse(connection.StopBits, true, out StopBits stopBits)
                || !Enum.IsDefined(stopBits)
            )
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' StopBits không hợp lệ: "
                        + $"{connection.StopBits}"
                );
            }
        }

        if (tcp)
        {
            if (string.IsNullOrWhiteSpace(connection.IpAddress))
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' ModbusTcp chưa có IpAddress."
                );
            }

            if (connection.Port <= 0 || connection.Port > 65535)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' "
                        + $"TCP Port không hợp lệ: {connection.Port}"
                );
            }
        }
    }

    // ============================================================
    // VALIDATE SLAVE / BLOCK / PARAMETER / STATUS / WRITE
    // ============================================================

    private static void ValidateSlave(
        string file,
        StationDefinition station,
        StationConnectionDefinition stationConnection,
        SlaveDefinition slave
    )
    {
        if (slave.Blocks is null || slave.Blocks.Count == 0)
        {
            throw new InvalidDataException(
                $"Station='{station.StationName}' " + $"Slave={slave.SlaveId} không có Blocks."
            );
        }

        HashSet<string> parameterNames = new(StringComparer.OrdinalIgnoreCase);

        foreach (RegisterBlockDefinition? block in slave.Blocks)
        {
            if (block is null)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' " + $"Slave={slave.SlaveId} chứa Block null."
                );
            }

            // Kiểm tra định dạng địa chỉ.
            _ = block.StartAddressValue;

            if (block.Quantity == 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Block={block.StartAddress} Quantity phải > 0."
                );
            }

            if (block.Function == 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Block={block.StartAddress} Function không hợp lệ."
                );
            }

            if (block.Parameters is null)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Block={block.StartAddress} Parameters null."
                );
            }

            if (block.Statuses is null)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Block={block.StartAddress} Statuses null."
                );
            }

            foreach (ParameterDefinition? parameter in block.Parameters)
            {
                if (parameter is null)
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' Slave={slave.SlaveId} "
                            + "chứa Parameter null."
                    );
                }

                if (string.IsNullOrWhiteSpace(parameter.Name))
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' Slave={slave.SlaveId} "
                            + "Parameter không có Name."
                    );
                }

                if (!parameterNames.Add(parameter.Name))
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' Slave={slave.SlaveId} "
                            + $"Parameter='{parameter.Name}' bị trùng."
                    );
                }

                if (parameter.Read is null)
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' "
                            + $"Parameter='{parameter.Name}' không có Read."
                    );
                }

                _ = parameter.Read.AddressValue;

                if (parameter.Read.Length <= 0)
                {
                    throw new InvalidDataException(
                        $"Parameter='{parameter.Name}' Read.Length phải > 0."
                    );
                }

                ValidateBits(
                    $"Parameter='{parameter.Name}'",
                    parameter.Read.Length,
                    parameter.Read.BitPosition,
                    parameter.Read.BitLength
                );
            }

            foreach (StatusDefinition? status in block.Statuses)
            {
                if (status is null)
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' Slave={slave.SlaveId} "
                            + "chứa Status null."
                    );
                }

                _ = status.AddressValue;

                if (string.IsNullOrWhiteSpace(status.Parameter))
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' Slave={slave.SlaveId} "
                            + $"Status={status.Address} chưa có Parameter."
                    );
                }

                if (!parameterNames.Contains(status.Parameter))
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' Slave={slave.SlaveId} "
                            + $"Status={status.Address} tham chiếu Parameter="
                            + $"'{status.Parameter}' không tồn tại."
                    );
                }

                if (status.Length <= 0)
                {
                    throw new InvalidDataException($"Status={status.Address} Length phải > 0.");
                }

                ValidateBits(
                    $"Status={status.Address}",
                    status.Length,
                    status.BitPosition,
                    status.BitLength
                );
            }
        }

        if (slave.Write is null)
            return;

        foreach (WriteDefinition? write in slave.Write)
        {
            if (write is null)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} " + "chứa Write null."
                );
            }

            _ = write.AddressValue;

            if (write.Source is null)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Write={write.Address} Source null."
                );
            }

            if (string.IsNullOrWhiteSpace(write.Source.Parameter))
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + "Write.Source.Parameter chưa cấu hình."
                );
            }

            if (write.Length <= 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Write={write.Address} Length phải > 0."
                );
            }

            if (write.Function == 0)
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Write={write.Address} Function không hợp lệ."
                );
            }

            if (string.IsNullOrWhiteSpace(write.Type))
            {
                throw new InvalidDataException(
                    $"Station='{station.StationName}' Slave={slave.SlaveId} "
                        + $"Write={write.Address} Type chưa cấu hình."
                );
            }
        }
    }

    // ============================================================
    // VALIDATE WRITE SOURCE
    // ============================================================

    private static void ValidateWrites(
        string file,
        StationDefinition station,
        StationConnectionDefinition stationConnection
    )
    {
        HashSet<string> parameters = stationConnection
            .Sensors.Where(s => s is not null)
            .SelectMany(s => s.Blocks ?? new())
            .Where(b => b is not null)
            .SelectMany(b => b.Parameters ?? new())
            .Where(p => p is not null && !string.IsNullOrWhiteSpace(p.Name))
            .Select(p => p.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        string connectionKey = GetConnectionKey(stationConnection.Connection);

        foreach (SlaveDefinition slave in stationConnection.Sensors)
        {
            if (slave.Write is null)
                continue;

            foreach (WriteDefinition write in slave.Write)
            {
                string source = write.Source!.Parameter;

                if (!parameters.Contains(source))
                {
                    throw new InvalidDataException(
                        $"Station='{station.StationName}' "
                            + $"Connection='{connectionKey}' "
                            + $"Write={write.Address} "
                            + $"Source.Parameter='{source}' không tồn tại."
                    );
                }

                Console.WriteLine(
                    $"Write OK | Station={station.StationName} | "
                        + $"Connection={connectionKey} | Slave={slave.SlaveId} | "
                        + $"Address={write.Address} | Source={source}"
                );
            }
        }
    }

    // ============================================================
    // VALIDATE BIT FIELD
    // ============================================================

    private static void ValidateBits(string target, int length, int? bitPosition, int? bitLength)
    {
        if (bitPosition.HasValue != bitLength.HasValue)
        {
            throw new InvalidDataException(
                $"{target}: BitPosition và BitLength phải đi cùng nhau."
            );
        }

        if (!bitPosition.HasValue)
            return;

        if (bitPosition.Value < 0)
        {
            throw new InvalidDataException($"{target}: BitPosition không được < 0.");
        }

        if (bitLength!.Value <= 0 || bitLength.Value > 64)
        {
            throw new InvalidDataException($"{target}: BitLength phải từ 1 đến 64.");
        }

        if (bitPosition.Value + bitLength.Value > length * 16)
        {
            throw new InvalidDataException($"{target}: Bit field vượt quá Length.");
        }
    }

    // ============================================================
    // CONNECTION KEY
    // ============================================================

    private static string GetConnectionKey(ConnectionDefinition connection)
    {
        if (connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase))
        {
            return $"tcp:{connection.IpAddress}:{connection.Port}";
        }

        return $"rtu:{connection.PortName}:"
            + $"{connection.BaudRate}:"
            + $"{connection.Parity}:"
            + $"{connection.DataBits}:"
            + $"{connection.StopBits}";
    }
}
