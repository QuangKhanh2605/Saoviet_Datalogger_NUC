using Microsoft.Extensions.Logging;
using Modbus.Decoders;
using Modbus.Hardware;
using Modbus.Models;

namespace Modbus.Sensors;

public class StationReader
{
    private readonly DecoderFactory _decoderFactory;
    private readonly ILogger<StationReader> _logger;

    public StationReader(DecoderFactory decoderFactory, ILogger<StationReader> logger)
    {
        _decoderFactory = decoderFactory;
        _logger = logger;
    }

    // ============================================================
    // RESULT
    // ============================================================

    public sealed class StationReadResult
    {
        public List<SensorReading> Readings { get; } = new();

        public List<(byte SlaveId, ParameterDefinition Parameter)> FailedParameters { get; } =
            new();
    }

    private sealed class BlockResult
    {
        public RegisterBlockDefinition Block { get; }
        public ushort[] Registers { get; }
        public Exception? Exception { get; }

        public bool Success => Exception == null;

        public BlockResult(
            RegisterBlockDefinition block,
            ushort[] registers,
            Exception? exception = null
        )
        {
            Block = block;
            Registers = registers;
            Exception = exception;
        }
    }

    // ============================================================
    // READ STATION
    // ============================================================

    public StationReadResult ReadStation(
        string stationName,
        List<SlaveDefinition> slaves,
        IModbusClient client
    )
    {
        StationReadResult result = new();

        foreach (var slave in slaves)
        {
            ReadSlave(stationName, slave, client, result);
        }

        foreach (var slave in slaves)
        {
            foreach (var write in slave.Write)
            {
                Write(stationName, slave, write, client, result);
            }
        }

        return result;
    }

    // ============================================================
    // READ SLAVE
    // ============================================================

    private void ReadSlave(
        string stationName,
        SlaveDefinition slave,
        IModbusClient client,
        StationReadResult result
    )
    {
        List<BlockResult> blocks = new();

        // --------------------------------------------------------
        // READ ALL BLOCKS
        // --------------------------------------------------------

        foreach (var block in slave.Blocks)
        {
            BlockResult blockResult = ReadBlock(stationName, slave.SlaveId, block, client);

            blocks.Add(blockResult);

            // ----------------------------------------------------
            // BLOCK FAILED
            // ----------------------------------------------------

            if (!blockResult.Success)
            {
                // Parameter trong block này bị lỗi.
                foreach (var parameter in block.Parameters)
                {
                    result.FailedParameters.Add((slave.SlaveId, parameter));

                    _logger.LogWarning(
                        "PARAMETER UNAVAILABLE | Station={Station} | Slave={Slave} | Parameter={Parameter}",
                        stationName,
                        slave.SlaveId,
                        parameter.Name
                    );
                }

                foreach (var status in block.Statuses)
                {
                    _logger.LogWarning(
                        "STATUS UNAVAILABLE | Station={Station} | Slave={Slave} | Parameter={Parameter}",
                        stationName,
                        slave.SlaveId,
                        status.Parameter
                    );
                }
            }
        }

        // --------------------------------------------------------
        // BUILD PARAMETER LIST
        // --------------------------------------------------------

        Dictionary<
            string,
            (ParameterDefinition Parameter, RegisterBlockDefinition Block)
        > parameters = new(StringComparer.OrdinalIgnoreCase);

        foreach (var block in slave.Blocks)
        {
            foreach (var parameter in block.Parameters)
            {
                if (!parameters.ContainsKey(parameter.Name))
                {
                    parameters[parameter.Name] = (parameter, block);
                }
            }
        }

        // --------------------------------------------------------
        // DECODE PARAMETERS
        // --------------------------------------------------------

        foreach (var item in parameters.Values)
        {
            ParameterDefinition parameter = item.Parameter;

            BlockResult? parameterBlock = FindBlock(blocks, item.Block);

            if (parameterBlock == null || !parameterBlock.Success)
            {
                continue;
            }

            // ----------------------------------------------------
            // FIND STATUS
            // ----------------------------------------------------

            var statuses = FindStatuses(slave, parameter.Name);

            try
            {
                SensorReading reading = DecodeParameter(
                    stationName,
                    slave.SlaveId,
                    parameter,
                    parameterBlock.Registers,
                    item.Block.StartAddressValue,
                    statuses,
                    blocks
                );

                result.Readings.Add(reading);

                LogReading(reading);
            }
            catch (Exception ex)
            {
                result.FailedParameters.Add((slave.SlaveId, parameter));

                _logger.LogWarning(
                    ex,
                    "PARAMETER DECODE ERROR | Station={Station} | Slave={Slave} | Parameter={Parameter}",
                    stationName,
                    slave.SlaveId,
                    parameter.Name
                );
            }
        }
    }

    // ============================================================
    // READ BLOCK
    // ============================================================

    private BlockResult ReadBlock(
        string stationName,
        byte slaveId,
        RegisterBlockDefinition block,
        IModbusClient client
    )
    {
        try
        {
            ushort[] registers = client.ReadRegisters(
                slaveId,
                block.Function,
                block.StartAddressValue,
                block.Quantity
            );

            return new BlockResult(block, registers);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "MODBUS READ ERROR | Station={Station} | Slave={Slave} | Address=0x{Address:X4} | Quantity={Quantity}",
                stationName,
                slaveId,
                block.StartAddressValue,
                block.Quantity
            );

            return new BlockResult(block, Array.Empty<ushort>(), ex);
        }
    }

    // ============================================================
    // WRITE
    // ============================================================

    private void Write(
        string stationName,
        SlaveDefinition slave,
        WriteDefinition write,
        IModbusClient client,
        StationReadResult result
    )
    {
        try
        {
            double value = GetWriteSourceValue(write, result.Readings);

            value *= write.Scale;

            ushort[] registers = RegisterEncoder.Encode(write.Type, value, write.ByteOrder);

            client.WriteRegisters(slave.SlaveId, write.Function, write.AddressValue, registers);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "MODBUS WRITE ERROR | Station={Station} | Slave={Slave} | Address=0x{Address:X4}",
                stationName,
                slave.SlaveId,
                write.AddressValue
            );
        }
    }

    private static double GetWriteSourceValue(WriteDefinition write, List<SensorReading> readings)
    {
        SensorReading? source = readings.FirstOrDefault(x =>
            x.Name.Equals(write.Source.Parameter, StringComparison.OrdinalIgnoreCase)
        );

        return source?.Status == StatusCode.Ok ? source.Value : write.FallbackValue;
    }

    // ============================================================
    // DECODE PARAMETER
    // ============================================================

    private SensorReading DecodeParameter(
        string stationName,
        byte slaveId,
        ParameterDefinition parameter,
        ushort[] parameterRegisters,
        ushort parameterBlockStart,
        List<(StatusDefinition Status, RegisterBlockDefinition Block)> statuses,
        List<BlockResult> blocks
    )
    {
        // ========================================================
        // VALUE
        // ========================================================

        ushort[] registers = GetRegisters(
            parameterRegisters,
            parameterBlockStart,
            parameter.Read.AddressValue,
            parameter.Read.Length
        );

        double value = parameter.Read.BitPosition.HasValue
            ? _decoderFactory.DecodeBits(
                registers,
                parameter.Read.BitPosition.Value,
                parameter.Read.BitLength!.Value,
                parameter.Read.ByteOrder,
                parameter.Read.BitOrder
            )
            : _decoderFactory.Decode(
                parameter.Read.Type,
                registers,
                parameter.Read.ByteOrder,
                parameter.Read.BitOrder
            );

        value = value * parameter.Read.Scale + parameter.Read.OffsetValue;

        // ========================================================
        // STATUS
        // ========================================================

        int status = StatusCode.Ok;

        List<int> statusCodes = new();

        foreach (var statusEntry in statuses)
        {
            BlockResult? statusBlock = FindBlock(blocks, statusEntry.Block);

            if (statusBlock == null || !statusBlock.Success)
            {
                _logger.LogDebug(
                    "STATUS SKIPPED | Station={Station} | Slave={Slave} | Parameter={Parameter} | StatusAddress=0x{Address:X4}",
                    stationName,
                    slaveId,
                    parameter.Name,
                    statusEntry.Status.AddressValue
                );

                continue;
            }

            try
            {
                // ------------------------------------------------
                // GET STATUS REGISTERS
                // ------------------------------------------------

                ushort[] statusRegisters = GetRegisters(
                    statusBlock.Registers,
                    statusEntry.Block.StartAddressValue,
                    statusEntry.Status.AddressValue,
                    statusEntry.Status.Length
                );

                // ------------------------------------------------
                // DECODE STATUS
                // ------------------------------------------------

                double raw = statusEntry.Status.BitPosition.HasValue
                    ? _decoderFactory.DecodeBits(
                        statusRegisters,
                        statusEntry.Status.BitPosition.Value,
                        statusEntry.Status.BitLength!.Value,
                        statusEntry.Status.ByteOrder,
                        statusEntry.Status.BitOrder
                    )
                    : _decoderFactory.Decode(
                        statusEntry.Status.Type,
                        statusRegisters,
                        statusEntry.Status.ByteOrder,
                        statusEntry.Status.BitOrder
                    );

                int statusCode = StatusRuleEvaluator.Evaluate(
                    checked((int)raw),
                    statusEntry.Status.Rules
                );

                statusCodes.Add(statusCode);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "STATUS DECODE ERROR | Station={Station} | Slave={Slave} | Parameter={Parameter} | StatusAddress=0x{Address:X4}",
                    stationName,
                    slaveId,
                    parameter.Name,
                    statusEntry.Status.AddressValue
                );
            }
        }

        // ========================================================
        // COMBINE AVAILABLE STATUS
        // ========================================================

        if (statusCodes.Count > 0)
        {
            status = StatusRuleEvaluator.Combine(statusCodes);
        }

        // ========================================================
        // RESULT
        // ========================================================

        return new SensorReading
        {
            StationName = stationName,
            SlaveId = slaveId,
            Name = parameter.Name,
            Value = Math.Round(value, 3),
            Unit = parameter.Unit,
            Status = status,
            Timestamp = DateTime.Now.ToString("yyyyMMddHHmmss"),
        };
    }

    // ============================================================
    // STATUS
    // ============================================================

    private static List<(StatusDefinition Status, RegisterBlockDefinition Block)> FindStatuses(
        SlaveDefinition slave,
        string parameterName
    )
    {
        List<(StatusDefinition Status, RegisterBlockDefinition Block)> result = new();

        foreach (var block in slave.Blocks)
        {
            foreach (var status in block.Statuses)
            {
                if (status.Parameter.Equals(parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    result.Add((status, block));
                }
            }
        }

        return result;
    }

    private static BlockResult? FindBlock(List<BlockResult> blocks, RegisterBlockDefinition block)
    {
        return blocks.FirstOrDefault(x => ReferenceEquals(x.Block, block));
    }

    // ============================================================
    // LOG
    // ============================================================

    private void LogReading(SensorReading reading)
    {
        if (reading.Status == StatusCode.Ok)
        {
            _logger.LogInformation(
                "READ OK | Station={Station} | Slave={Slave} | Parameter={Parameter} | Value={Value} | Unit={Unit}",
                reading.StationName,
                reading.SlaveId,
                reading.Name,
                reading.Value,
                reading.Unit
            );
        }
        else
        {
            _logger.LogWarning(
                "READ ERROR | Station={Station} | Slave={Slave} | Parameter={Parameter} | Value={Value} | Status={Status}",
                reading.StationName,
                reading.SlaveId,
                reading.Name,
                reading.Value,
                reading.Status
            );
        }
    }

    // ============================================================
    // REGISTER EXTRACTION
    // ============================================================

    private static ushort[] GetRegisters(
        ushort[] blockRegisters,
        ushort blockStart,
        ushort address,
        int length
    )
    {
        if (length <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        int index = address - blockStart;

        if (index < 0 || index + length > blockRegisters.Length)
        {
            throw new Exception(
                $"Address=0x{address:X4}, Length={length} nằm ngoài Block=0x{blockStart:X4}, Quantity={blockRegisters.Length}."
            );
        }

        ushort[] result = new ushort[length];

        Array.Copy(blockRegisters, index, result, 0, length);

        return result;
    }
}
