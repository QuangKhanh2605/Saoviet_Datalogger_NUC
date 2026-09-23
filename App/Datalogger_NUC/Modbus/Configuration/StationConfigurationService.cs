using System.Text.Json;
using Modbus.Sensors;

namespace Modbus.Configuration;

public class StationConfigurationService
{
    private readonly string _directoryPath;

    public StationConfigurationService(string directoryPath)
    {
        _directoryPath = directoryPath;
    }

    public List<StationDefinition> LoadAll()
    {
        if (!Directory.Exists(_directoryPath))
            throw new DirectoryNotFoundException(
                $"Không tìm thấy thư mục station: {_directoryPath}"
            );

        string[] files = Directory.GetFiles(
            _directoryPath,
            "*.json",
            SearchOption.TopDirectoryOnly
        );

        if (files.Length == 0)
            throw new FileNotFoundException($"Không tìm thấy file JSON trong: {_directoryPath}");

        if (files.Length > 1)
            throw new InvalidOperationException(
                $"Thư mục station phải chứa đúng 1 file JSON. "
                    + $"Tìm thấy: {string.Join(", ", files.Select(Path.GetFileName))}"
            );

        string file = files[0];

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        StationConfiguration configuration;

        try
        {
            string json = File.ReadAllText(file);

            configuration =
                JsonSerializer.Deserialize<StationConfiguration>(json, options)
                ?? throw new Exception("Deserialize trả về null.");
        }
        catch (JsonException ex)
        {
            throw new Exception($"JSON station configuration không hợp lệ: {ex.Message}", ex);
        }

        if (configuration.Stations == null)
            throw new Exception("Station configuration không có 'Stations'.");

        List<StationDefinition> stations = new();

        foreach (StationDefinition station in configuration.Stations)
        {
            try
            {
                if (!station.Enabled)
                {
                    Console.WriteLine($"Bỏ qua station disabled: {station.StationName}");

                    continue;
                }

                ValidateStation(file, station);
                stations.Add(station);

                Console.WriteLine($"Station configuration OK: Station={station.StationName}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Bỏ qua station '{station.StationName}': {ex.Message}");
            }
        }

        stations = ValidateSharedConnections(stations);

        Console.WriteLine(
            $"Load station hoàn tất. "
                + $"ValidStations={stations.Count} "
                + $"TotalStations={configuration.Stations.Count}"
        );

        return stations;
    }

    private static void ValidateStation(string file, StationDefinition station)
    {
        if (string.IsNullOrWhiteSpace(station.StationName))
            throw new Exception("StationName chưa được cấu hình.");

        ConnectionDefinition connection =
            station.Connection
            ?? throw new Exception($"Station '{station.StationName}' chưa có Connection.");

        if (
            string.IsNullOrWhiteSpace(connection.PortName)
            && string.IsNullOrWhiteSpace(connection.IpAddress)
        )
            throw new Exception($"Station '{station.StationName}' chưa có PortName/IpAddress.");

        if (string.IsNullOrWhiteSpace(connection.Protocol))
            throw new Exception($"Station '{station.StationName}' chưa có Protocol.");

        if (
            !connection.Protocol.Equals("ModbusRtu", StringComparison.OrdinalIgnoreCase)
            && !connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase)
        )
            throw new Exception(
                $"Station '{station.StationName}' có Protocol không hỗ trợ: "
                    + $"{connection.Protocol}"
            );

        if (station.Sensors == null || station.Sensors.Count == 0)
            throw new Exception($"Station '{station.StationName}' không có Sensors.");

        foreach (SlaveDefinition slave in station.Sensors)
            ValidateSlave(file, slave);

        ValidateStationWrites(file, station);
    }

    private static void ValidateSlave(string file, SlaveDefinition slave)
    {
        if (slave.Blocks == null || slave.Blocks.Count == 0)
            throw new Exception($"File '{file}': SlaveId={slave.SlaveId} không có Blocks.");

        foreach (RegisterBlockDefinition block in slave.Blocks)
            ValidateBlock(file, slave, block);

        ValidateParameterNames(file, slave);
        ValidateStatusReferences(file, slave);

        if (slave.Write == null)
            return;

        foreach (WriteDefinition write in slave.Write)
            ValidateWriteStructure(file, slave, write);
    }

    private static void ValidateParameterNames(string file, SlaveDefinition slave)
    {
        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);

        foreach (RegisterBlockDefinition block in slave.Blocks)
        {
            foreach (ParameterDefinition parameter in block.Parameters)
            {
                if (!names.Add(parameter.Name))
                    throw new Exception(
                        $"File '{file}': SlaveId={slave.SlaveId} "
                            + $"Parameter '{parameter.Name}' bị trùng."
                    );
            }
        }
    }

    private static void ValidateStatusReferences(string file, SlaveDefinition slave)
    {
        HashSet<string> parameters = slave
            .Blocks.SelectMany(block => block.Parameters)
            .Select(parameter => parameter.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (RegisterBlockDefinition block in slave.Blocks)
        {
            foreach (StatusDefinition status in block.Statuses)
            {
                if (string.IsNullOrWhiteSpace(status.Parameter))
                    throw new Exception(
                        $"File '{file}': SlaveId={slave.SlaveId} "
                            + $"Status Address={status.Address} chưa có Parameter."
                    );

                if (!parameters.Contains(status.Parameter))
                    throw new Exception(
                        $"File '{file}': SlaveId={slave.SlaveId} "
                            + $"Status Address={status.Address} "
                            + $"tham chiếu Parameter='{status.Parameter}' không tồn tại."
                    );
            }
        }
    }

    private static void ValidateBlock(
        string file,
        SlaveDefinition slave,
        RegisterBlockDefinition block
    )
    {
        if (block.Quantity <= 0)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Block {block.StartAddress}: Quantity phải > 0."
            );

        if (block.Function <= 0)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Block {block.StartAddress}: Function không hợp lệ."
            );

        if (block.Parameters == null)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Block {block.StartAddress}: Parameters bị null."
            );

        if (block.Statuses == null)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Block {block.StartAddress}: Statuses bị null."
            );

        foreach (ParameterDefinition parameter in block.Parameters)
            ValidateParameter(file, slave, parameter);

        foreach (StatusDefinition status in block.Statuses)
            ValidateStatus(file, slave, block, status);
    }

    private static void ValidateParameter(
        string file,
        SlaveDefinition slave,
        ParameterDefinition parameter
    )
    {
        if (string.IsNullOrWhiteSpace(parameter.Name))
            throw new Exception($"File '{file}': SlaveId={slave.SlaveId} Parameter không có Name.");

        if (parameter.Read == null)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Parameter='{parameter.Name}' không có Read."
            );

        if (parameter.Read.Length <= 0)
            throw new Exception(
                $"File '{file}': Parameter='{parameter.Name}' " + $"Read.Length phải > 0."
            );

        ValidateBitField(
            file,
            $"Parameter='{parameter.Name}'",
            parameter.Read.Length,
            parameter.Read.BitPosition,
            parameter.Read.BitLength
        );
    }

    private static void ValidateStatus(
        string file,
        SlaveDefinition slave,
        RegisterBlockDefinition block,
        StatusDefinition status
    )
    {
        if (string.IsNullOrWhiteSpace(status.Parameter))
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Status Address={status.Address} chưa có Parameter."
            );

        if (status.Length <= 0)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Status Address={status.Address} Length phải > 0."
            );

        ValidateBitField(
            file,
            $"Status Address={status.Address}",
            status.Length,
            status.BitPosition,
            status.BitLength
        );
    }

    private static void ValidateBitField(
        string file,
        string target,
        int length,
        int? bitPosition,
        int? bitLength
    )
    {
        if (bitPosition.HasValue != bitLength.HasValue)
            throw new Exception(
                $"File '{file}': {target} " + "phải khai báo đồng thời BitPosition và BitLength."
            );

        if (!bitPosition.HasValue)
            return;

        if (bitPosition.Value < 0)
            throw new Exception($"File '{file}': {target} BitPosition không được < 0.");

        if (bitLength!.Value <= 0 || bitLength.Value > 64)
            throw new Exception($"File '{file}': {target} BitLength phải từ 1 đến 64.");

        if (bitPosition.Value + bitLength.Value > length * 16)
            throw new Exception($"File '{file}': {target} Bit field vượt quá Length.");
    }

    private static void ValidateWriteStructure(
        string file,
        SlaveDefinition slave,
        WriteDefinition write
    )
    {
        if (write.Source == null)
            throw new Exception($"File '{file}': SlaveId={slave.SlaveId} Write.Source bị null.");

        if (string.IsNullOrWhiteSpace(write.Source.Parameter))
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + "Write.Source.Parameter chưa được cấu hình."
            );

        if (write.Length <= 0)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Write Address={write.Address} Length phải > 0."
            );

        if (write.Function <= 0)
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Write Address={write.Address} Function không hợp lệ."
            );

        if (string.IsNullOrWhiteSpace(write.Type))
            throw new Exception(
                $"File '{file}': SlaveId={slave.SlaveId} "
                    + $"Write Address={write.Address} Type chưa được cấu hình."
            );
    }

    private static void ValidateStationWrites(string file, StationDefinition station)
    {
        HashSet<string> parameters = station
            .Sensors.SelectMany(slave => slave.Blocks)
            .SelectMany(block => block.Parameters)
            .Select(parameter => parameter.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (SlaveDefinition slave in station.Sensors)
        {
            if (slave.Write == null)
                continue;

            foreach (WriteDefinition write in slave.Write)
            {
                string source = write.Source.Parameter;

                if (!parameters.Contains(source))
                    throw new Exception(
                        $"File '{file}': Station='{station.StationName}' "
                            + $"Write Address={write.Address} "
                            + $"Source.Parameter='{source}' không tồn tại."
                    );

                Console.WriteLine(
                    $"Write source OK | Station={station.StationName} "
                        + $"| WriteSlave={slave.SlaveId} "
                        + $"| WriteAddress={write.Address} "
                        + $"| Source={source}"
                );
            }
        }
    }

    private static List<StationDefinition> ValidateSharedConnections(
        List<StationDefinition> stations
    )
    {
        List<StationDefinition> valid = new();

        foreach (
            var group in stations.GroupBy(
                s => GetConnectionKey(s.Connection),
                StringComparer.OrdinalIgnoreCase
            )
        )
        {
            StationDefinition reference = group.First();

            valid.Add(reference);

            foreach (StationDefinition station in group.Skip(1))
            {
                if (IsSameConnection(reference.Connection, station.Connection))
                {
                    valid.Add(station);
                }
                else
                {
                    Console.WriteLine(
                        $"[ERROR] Bỏ qua station '{station.StationName}'. "
                            + $"Connection không giống '{reference.StationName}'."
                    );
                }
            }
        }

        return valid;
    }

    private static bool IsSameConnection(ConnectionDefinition first, ConnectionDefinition second)
    {
        if (!string.Equals(first.Protocol, second.Protocol, StringComparison.OrdinalIgnoreCase))
            return false;

        if (first.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase))
        {
            return string.Equals(
                    first.IpAddress,
                    second.IpAddress,
                    StringComparison.OrdinalIgnoreCase
                )
                && first.Port == second.Port;
        }

        return string.Equals(first.PortName, second.PortName, StringComparison.OrdinalIgnoreCase)
            && first.BaudRate == second.BaudRate
            && string.Equals(first.Parity, second.Parity, StringComparison.OrdinalIgnoreCase)
            && first.DataBits == second.DataBits
            && string.Equals(first.StopBits, second.StopBits, StringComparison.OrdinalIgnoreCase);
    }

    private static string GetConnectionKey(ConnectionDefinition connection)
    {
        return connection.Protocol.Equals("ModbusTcp", StringComparison.OrdinalIgnoreCase)
            ? $"tcp:{connection.IpAddress}:{connection.Port}"
            : $"rtu:{connection.PortName}";
    }
}
