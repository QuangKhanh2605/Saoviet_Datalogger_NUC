using Datalogger_Linux.Decoders;
using Datalogger_Linux.Hardware;
using Datalogger_Linux.Models;
using Microsoft.Extensions.Logging;

namespace Datalogger_Linux.Sensors;

public class StationReader
{
    private readonly DecoderFactory _decoderFactory;
    private readonly ILogger<StationReader> _logger;

    public StationReader(DecoderFactory decoderFactory, ILogger<StationReader> logger)
    {
        _decoderFactory = decoderFactory;
        _logger = logger;
    }

    // =========================================================
    // RESULT CỦA MỘT STATION
    // =========================================================

    public sealed class StationReadResult
    {
        // Parameter đọc thành công hoặc đọc được nhưng
        // device-reported Status = Error/Calibrating.
        public List<SensorReading> Readings { get; } = new();

        // Parameter không đọc được do Modbus/block/configuration
        // làm cho parameter không thể cung cấp reading hợp lệ.
        public List<SlaveReadError> Errors { get; } = new();
    }

    // =========================================================
    // LỖI CỦA BLOCK
    // =========================================================

    public sealed class SlaveReadError
    {
        public byte SlaveId { get; }

        public ushort StartAddress { get; }

        public ushort Quantity { get; }

        public Exception Exception { get; }

        public string Message { get; }

        public List<ParameterDefinition> Parameters { get; }

        public SlaveReadError(
            byte slaveId,
            ushort startAddress,
            ushort quantity,
            Exception exception,
            string message,
            List<ParameterDefinition> parameters
        )
        {
            SlaveId = slaveId;
            StartAddress = startAddress;
            Quantity = quantity;
            Exception = exception;
            Message = message;
            Parameters = parameters;
        }
    }

    // =========================================================
    // INTERNAL RESULT CỦA BLOCK
    // =========================================================

    private sealed class BlockReadResult
    {
        public RegisterBlockDefinition Block { get; }

        public bool Success { get; }

        public ushort[] Registers { get; }

        public Exception? Exception { get; }

        public BlockReadResult(
            RegisterBlockDefinition block,
            bool success,
            ushort[] registers,
            Exception? exception
        )
        {
            Block = block;
            Success = success;
            Registers = registers;
            Exception = exception;
        }
    }

    // =========================================================
    // READ STATION
    // =========================================================

    public StationReadResult ReadStation(
        string stationName,
        List<SlaveDefinition> slaves,
        IModbusClient client
    )
    {
        StationReadResult result = new();

        // =====================================================
        // READ
        // =====================================================

        foreach (SlaveDefinition slave in slaves)
        {
            ReadSlave(stationName, slave, client, result);
        }

        // =====================================================
        // WRITE
        // =====================================================

        foreach (SlaveDefinition slave in slaves)
        {
            foreach (WriteDefinition write in slave.Write)
            {
                try
                {
                    double valueToWrite = GetWriteSourceValue(write, result.Readings);

                    valueToWrite *= write.Scale;

                    ushort[] registers = RegisterEncoder.Encode(
                        write.Type,
                        valueToWrite,
                        write.ByteOrder
                    );

                    client.WriteRegisters(
                        slave.SlaveId,
                        write.Function,
                        write.AddressValue,
                        registers
                    );
                }
                catch (TimeoutException ex)
                {
                    AddOrReplaceSlaveError(
                        result.Errors,
                        slave.SlaveId,
                        0,
                        0,
                        ex,
                        "Timeout khi ghi Modbus.",
                        new List<ParameterDefinition>()
                    );
                }
                catch (IOException ex)
                {
                    AddOrReplaceSlaveError(
                        result.Errors,
                        slave.SlaveId,
                        0,
                        0,
                        ex,
                        "Lỗi I/O khi ghi Modbus.",
                        new List<ParameterDefinition>()
                    );
                }
                catch (InvalidOperationException ex)
                {
                    AddOrReplaceSlaveError(
                        result.Errors,
                        slave.SlaveId,
                        0,
                        0,
                        ex,
                        "Connection không hợp lệ khi ghi Modbus.",
                        new List<ParameterDefinition>()
                    );
                }
                catch (Exception ex)
                {
                    AddOrReplaceSlaveError(
                        result.Errors,
                        slave.SlaveId,
                        0,
                        0,
                        ex,
                        "Lỗi khi ghi Modbus.",
                        new List<ParameterDefinition>()
                    );
                }
            }
        }

        return result;
    }

    // =========================================================
    // GET WRITE SOURCE VALUE
    // =========================================================

    private static double GetWriteSourceValue(WriteDefinition write, List<SensorReading> readings)
    {
        SensorReading? source = readings.FirstOrDefault(x =>
            x.ParameterName.Equals(write.Source.Parameter, StringComparison.OrdinalIgnoreCase)
        );

        if (source != null && source.Status == StatusCode.Ok)
        {
            return source.Value;
        }

        return write.FallbackValue;
    }

    // =========================================================
    // ADD / REPLACE ERROR
    // =========================================================

    private static void AddOrReplaceSlaveError(
        List<SlaveReadError> errors,
        byte slaveId,
        ushort startAddress,
        ushort quantity,
        Exception exception,
        string message,
        List<ParameterDefinition> parameters
    )
    {
        int index = errors.FindIndex(e =>
            e.SlaveId == slaveId && e.StartAddress == startAddress && e.Quantity == quantity
        );

        SlaveReadError error = new(slaveId, startAddress, quantity, exception, message, parameters);

        if (index >= 0)
        {
            errors[index] = error;
        }
        else
        {
            errors.Add(error);
        }
    }

    // =========================================================
    // READ SLAVE
    // =========================================================

    private void ReadSlave(
        string stationName,
        SlaveDefinition slave,
        IModbusClient client,
        StationReadResult result
    )
    {
        // =====================================================
        // STEP 1:
        // READ TẤT CẢ BLOCK TRƯỚC
        // =====================================================

        List<BlockReadResult> blockResults = new();

        foreach (RegisterBlockDefinition block in slave.Blocks)
        {
            try
            {
                ushort[] registers = client.ReadRegisters(
                    slave.SlaveId,
                    block.Function,
                    block.StartAddressValue,
                    block.Quantity
                );

                blockResults.Add(new BlockReadResult(block, true, registers, null));
            }
            catch (TimeoutException ex)
            {
                LogBlockError(stationName, slave.SlaveId, block, "TIMEOUT", ex);

                AddOrReplaceSlaveError(
                    result.Errors,
                    slave.SlaveId,
                    block.StartAddressValue,
                    block.Quantity,
                    ex,
                    "Timeout khi đọc Block.",
                    block.Parameters
                );

                blockResults.Add(new BlockReadResult(block, false, Array.Empty<ushort>(), ex));
            }
            catch (IOException ex)
            {
                LogBlockError(stationName, slave.SlaveId, block, "IO ERROR", ex);

                AddOrReplaceSlaveError(
                    result.Errors,
                    slave.SlaveId,
                    block.StartAddressValue,
                    block.Quantity,
                    ex,
                    "Lỗi I/O khi đọc Block.",
                    block.Parameters
                );

                blockResults.Add(new BlockReadResult(block, false, Array.Empty<ushort>(), ex));
            }
            catch (InvalidOperationException ex)
            {
                LogBlockError(stationName, slave.SlaveId, block, "CONNECTION ERROR", ex);

                AddOrReplaceSlaveError(
                    result.Errors,
                    slave.SlaveId,
                    block.StartAddressValue,
                    block.Quantity,
                    ex,
                    "Connection không hợp lệ khi đọc Block.",
                    block.Parameters
                );

                blockResults.Add(new BlockReadResult(block, false, Array.Empty<ushort>(), ex));
            }
            catch (Exception ex)
            {
                LogBlockError(stationName, slave.SlaveId, block, "READ ERROR", ex);

                AddOrReplaceSlaveError(
                    result.Errors,
                    slave.SlaveId,
                    block.StartAddressValue,
                    block.Quantity,
                    ex,
                    "Lỗi khi đọc Block.",
                    block.Parameters
                );

                blockResults.Add(new BlockReadResult(block, false, Array.Empty<ushort>(), ex));
            }
        }

        // =====================================================
        // STEP 2:
        // TÌM PARAMETER VÀ BLOCK CHỨA PARAMETER
        // =====================================================

        Dictionary<
            string,
            (ParameterDefinition Parameter, RegisterBlockDefinition Block)
        > parametersByName = new(StringComparer.OrdinalIgnoreCase);

        foreach (RegisterBlockDefinition block in slave.Blocks)
        {
            foreach (ParameterDefinition parameter in block.Parameters)
            {
                if (!parametersByName.ContainsKey(parameter.Name))
                {
                    parametersByName.Add(parameter.Name, (parameter, block));
                }
            }
        }

        // =====================================================
        // STEP 3:
        // DECODE TỪNG PARAMETER
        // =====================================================

        foreach (var parameterEntry in parametersByName)
        {
            string parameterName = parameterEntry.Key;

            ParameterDefinition parameter = parameterEntry.Value.Parameter;

            RegisterBlockDefinition parameterBlock = parameterEntry.Value.Block;

            BlockReadResult? parameterBlockResult = FindBlockResult(blockResults, parameterBlock);

            // =================================================
            // PARAMETER BLOCK ĐỌC THẤT BẠI
            // =================================================

            if (parameterBlockResult == null || !parameterBlockResult.Success)
            {
                // Error của parameter block đã được thêm ở STEP 1.
                continue;
            }

            // =================================================
            // TÌM TẤT CẢ STATUS CỦA PARAMETER
            // =================================================

            List<(StatusDefinition Status, RegisterBlockDefinition Block)> statuses =
                FindStatusesForParameter(slave, parameterName);

            // =================================================
            // KIỂM TRA STATUS BLOCK
            // =================================================

            List<(StatusDefinition Status, RegisterBlockDefinition Block)> failedStatusBlocks =
                new();

            foreach (var statusEntry in statuses)
            {
                BlockReadResult? statusBlockResult = FindBlockResult(
                    blockResults,
                    statusEntry.Block
                );

                if (statusBlockResult == null || !statusBlockResult.Success)
                {
                    failedStatusBlocks.Add(statusEntry);
                }
            }

            // =================================================
            // STATUS BLOCK BỊ LỖI
            //
            // Không trả SensorReading.
            //
            // Parameter được đưa vào Error để ModbusWorker
            // xử lý theo cơ chế parameter error counter hiện tại.
            // =================================================

            if (failedStatusBlocks.Count > 0)
            {
                foreach (var failedStatusBlock in failedStatusBlocks)
                {
                    BlockReadResult? failedBlockResult = FindBlockResult(
                        blockResults,
                        failedStatusBlock.Block
                    );

                    if (failedBlockResult?.Exception == null)
                    {
                        continue;
                    }

                    AddParameterToBlockError(
                        result.Errors,
                        slave.SlaveId,
                        failedStatusBlock.Block,
                        failedBlockResult.Exception,
                        parameter
                    );

                    _logger.LogWarning(
                        "STATUS UNAVAILABLE | Station={Station} | Slave={Slave} | Parameter={Parameter} | StatusAddress=0x{Address:X4} | StatusBlock=0x{Block:X4}",
                        stationName,
                        slave.SlaveId,
                        parameter.Name,
                        failedStatusBlock.Status.AddressValue,
                        failedStatusBlock.Block.StartAddressValue
                    );
                }

                continue;
            }

            // =================================================
            // DECODE PARAMETER + STATUS
            // =================================================

            SensorReading reading = DecodeParameter(
                stationName,
                slave.SlaveId,
                parameter,
                parameterBlockResult.Registers,
                parameterBlock.StartAddressValue,
                statuses,
                blockResults
            );

            result.Readings.Add(reading);

            // =================================================
            // LOG SENSOR READING
            // =================================================

            if (reading.Status == StatusCode.Ok)
            {
                _logger.LogInformation(
                    "READ OK | Station={Station} | Slave={Slave} | Parameter={Parameter} | Value={Value} | Unit={Unit} | Status={Status}",
                    reading.StationName,
                    reading.SlaveId,
                    reading.ParameterName,
                    reading.Value,
                    reading.Unit,
                    reading.Status
                );
            }
            else
            {
                _logger.LogWarning(
                    "READ ERROR | Station={Station} | Slave={Slave} | Parameter={Parameter} | Value={Value} | Unit={Unit} | Status={Status}",
                    reading.StationName,
                    reading.SlaveId,
                    reading.ParameterName,
                    reading.Value,
                    reading.Unit,
                    reading.Status
                );
            }
        }
    }

    // =========================================================
    // FIND BLOCK RESULT
    // =========================================================

    private static BlockReadResult? FindBlockResult(
        List<BlockReadResult> blockResults,
        RegisterBlockDefinition block
    )
    {
        return blockResults.FirstOrDefault(x => ReferenceEquals(x.Block, block));
    }

    // =========================================================
    // FIND STATUSES FOR PARAMETER
    // =========================================================

    private static List<(
        StatusDefinition Status,
        RegisterBlockDefinition Block
    )> FindStatusesForParameter(SlaveDefinition slave, string parameterName)
    {
        List<(StatusDefinition Status, RegisterBlockDefinition Block)> result = new();

        foreach (RegisterBlockDefinition block in slave.Blocks)
        {
            foreach (StatusDefinition status in block.Statuses)
            {
                if (status.Parameter.Equals(parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    result.Add((status, block));
                }
            }
        }

        return result;
    }

    // =========================================================
    // ADD PARAMETER TO BLOCK ERROR
    // =========================================================

    private static void AddParameterToBlockError(
        List<SlaveReadError> errors,
        byte slaveId,
        RegisterBlockDefinition block,
        Exception exception,
        ParameterDefinition parameter
    )
    {
        int index = errors.FindIndex(e =>
            e.SlaveId == slaveId
            && e.StartAddress == block.StartAddressValue
            && e.Quantity == block.Quantity
        );

        if (index >= 0)
        {
            SlaveReadError existing = errors[index];

            if (
                !existing.Parameters.Any(x =>
                    x.Name.Equals(parameter.Name, StringComparison.OrdinalIgnoreCase)
                )
            )
            {
                existing.Parameters.Add(parameter);
            }

            return;
        }

        errors.Add(
            new SlaveReadError(
                slaveId,
                block.StartAddressValue,
                block.Quantity,
                exception,
                "Block chứa Status của Parameter bị lỗi.",
                new List<ParameterDefinition> { parameter }
            )
        );
    }

    // =========================================================
    // LOG BLOCK ERROR
    // =========================================================

    private void LogBlockError(
        string stationName,
        byte slaveId,
        RegisterBlockDefinition block,
        string errorType,
        Exception exception
    )
    {
        _logger.LogWarning(
            "MODBUS {ErrorType} | Station={Station} | Slave={Slave} | StartAddress=0x{Address:X4} | Quantity={Quantity} | Message={Message}",
            errorType,
            stationName,
            slaveId,
            block.StartAddressValue,
            block.Quantity,
            exception.Message
        );

        // Parameter trực tiếp thuộc block.
        foreach (ParameterDefinition parameter in block.Parameters)
        {
            _logger.LogWarning(
                "PARAMETER UNAVAILABLE | Station={Station} | Slave={Slave} | Parameter={Parameter} | Value=0 | Status={Status}",
                stationName,
                slaveId,
                parameter.Name,
                StatusCode.Error
            );
        }

        // Status thuộc block này có thể ảnh hưởng đến
        // Parameter nằm ở block khác.
        foreach (StatusDefinition status in block.Statuses)
        {
            _logger.LogWarning(
                "STATUS UNAVAILABLE | Station={Station} | Slave={Slave} | Parameter={Parameter} | StatusAddress=0x{Address:X4}",
                stationName,
                slaveId,
                status.Parameter,
                status.AddressValue
            );
        }
    }

    // =========================================================
    // DECODE PARAMETER
    // =========================================================

    private SensorReading DecodeParameter(
        string stationName,
        byte slaveId,
        ParameterDefinition parameter,
        ushort[] parameterBlockRegisters,
        ushort parameterBlockStartAddress,
        List<(StatusDefinition Status, RegisterBlockDefinition Block)> statuses,
        List<BlockReadResult> blockResults
    )
    {
        double value = 0;

        // Đọc thành công thì ban đầu OK.
        int status = StatusCode.Ok;

        try
        {
            // =================================================
            // GET VALUE REGISTERS
            // =================================================

            ushort[] valueRegisters = GetRegistersFromBlock(
                parameterBlockRegisters,
                parameterBlockStartAddress,
                parameter.Read.AddressValue,
                parameter.Read.Length
            );

            // =================================================
            // DECODE VALUE
            // =================================================

            if (parameter.Read.BitPosition.HasValue && parameter.Read.BitLength.HasValue)
            {
                value = _decoderFactory.DecodeBits(
                    valueRegisters,
                    parameter.Read.BitPosition.Value,
                    parameter.Read.BitLength.Value,
                    parameter.Read.ByteOrder,
                    parameter.Read.BitOrder
                );
            }
            else
            {
                value = _decoderFactory.Decode(
                    parameter.Read.Type,
                    valueRegisters,
                    parameter.Read.ByteOrder,
                    parameter.Read.BitOrder
                );
            }

            // =================================================
            // SCALE + OFFSET
            // =================================================

            value = value * parameter.Read.Scale + parameter.Read.OffsetValue;

            // =================================================
            // STATUS RULE
            // =================================================

            List<int> statusCodes = new();

            foreach (var statusEntry in statuses)
            {
                StatusDefinition statusDef = statusEntry.Status;

                RegisterBlockDefinition statusBlock = statusEntry.Block;

                BlockReadResult? statusBlockResult = FindBlockResult(blockResults, statusBlock);

                if (statusBlockResult == null || !statusBlockResult.Success)
                {
                    // Trường hợp này đã được xử lý trước
                    // ở ReadSlave().
                    throw new Exception(
                        $"Status block 0x{statusBlock.StartAddressValue:X4} " + $"không có dữ liệu."
                    );
                }

                // =================================================
                // GET STATUS REGISTERS
                // =================================================

                ushort[] statusRegisters = GetRegistersFromBlock(
                    statusBlockResult.Registers,
                    statusBlock.StartAddressValue,
                    statusDef.AddressValue,
                    statusDef.Length
                );

                // =================================================
                // DECODE STATUS
                // =================================================

                double decodedStatus;

                if (statusDef.BitPosition.HasValue && statusDef.BitLength.HasValue)
                {
                    decodedStatus = _decoderFactory.DecodeBits(
                        statusRegisters,
                        statusDef.BitPosition.Value,
                        statusDef.BitLength.Value,
                        statusDef.ByteOrder,
                        statusDef.BitOrder
                    );
                }
                else
                {
                    decodedStatus = _decoderFactory.Decode(
                        statusDef.Type,
                        statusRegisters,
                        statusDef.ByteOrder,
                        statusDef.BitOrder
                    );
                }

                int rawValue = checked((int)decodedStatus);

                int evaluated = StatusRuleEvaluator.Evaluate(rawValue, statusDef.Rules);

                statusCodes.Add(evaluated);
            }

            // =================================================
            // COMBINE STATUS
            // =================================================

            if (statusCodes.Count > 0)
            {
                status = StatusRuleEvaluator.Combine(statusCodes);
            }
            else
            {
                status = StatusCode.Ok;
            }
        }
        catch
        {
            // Decode/configuration lỗi.
            value = 0;
            status = StatusCode.Error;
        }

        return new SensorReading
        {
            StationName = stationName,

            SlaveId = slaveId,

            ParameterName = parameter.Name,

            Value = Math.Round(value, 3),

            Unit = parameter.Unit,

            Status = status,

            Timestamp = DateTime.Now.ToString("yyyyMMddHHmmss"),
        };
    }

    // =========================================================
    // GET REGISTERS FROM BLOCK
    // =========================================================

    private static ushort[] GetRegistersFromBlock(
        ushort[] blockRegisters,
        ushort blockStartAddress,
        ushort parameterAddress,
        int length
    )
    {
        if (length <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length),
                "Register length phải lớn hơn 0."
            );
        }

        int startIndex = parameterAddress - blockStartAddress;

        if (startIndex < 0)
        {
            throw new Exception(
                $"Address 0x{parameterAddress:X4} "
                    + "nằm trước Block "
                    + $"0x{blockStartAddress:X4}."
            );
        }

        if (startIndex + length > blockRegisters.Length)
        {
            throw new Exception(
                $"Address 0x{parameterAddress:X4}, "
                    + $"Length={length} "
                    + "vượt ra ngoài Block "
                    + $"0x{blockStartAddress:X4}, "
                    + $"Quantity={blockRegisters.Length}."
            );
        }

        ushort[] result = new ushort[length];

        Array.Copy(blockRegisters, startIndex, result, 0, length);

        return result;
    }
}
