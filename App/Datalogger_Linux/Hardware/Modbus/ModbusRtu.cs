using System.IO.Ports;

namespace Datalogger_Linux.Hardware;

public class ModbusRtu : IModbusClient
{
    // SERIAL SETTINGS
    private const int ReadTimeoutMilliseconds = 1000;
    private const int WriteTimeoutMilliseconds = 1000;

    // Ở 9600 baud:
    // 1 character ~ 1.04 ms với 10 bit/character.
    // 3.5 character ~ 3.65 ms.
    private const int InterFrameDelayMilliseconds = 5;

    private const int RetryDelayMilliseconds = 20;

    private const int MaxRetries = 2;

    private const ushort MaxReadRegisters = 125;
    private const ushort MaxWriteRegisters = 123;

    private readonly SerialPort _serialPort;

    public ModbusRtu(string portName, int baudRate, Parity parity, int dataBits, StopBits stopBits)
    {
        if (string.IsNullOrWhiteSpace(portName))
        {
            throw new ArgumentException("PortName không được rỗng.", nameof(portName));
        }

        if (baudRate <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(baudRate), "BaudRate phải lớn hơn 0.");
        }

        if (dataBits <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(dataBits), "DataBits phải lớn hơn 0.");
        }

        _serialPort = new SerialPort(portName, baudRate, parity, dataBits, stopBits);

        _serialPort.ReadTimeout = ReadTimeoutMilliseconds;
        _serialPort.WriteTimeout = WriteTimeoutMilliseconds;
    }

    // OPEN
    public void Open()
    {
        if (_serialPort.IsOpen)
        {
            return;
        }

        _serialPort.Open();

        _serialPort.DiscardInBuffer();
        _serialPort.DiscardOutBuffer();
    }

    // =========================================================
    // CLOSE
    // =========================================================

    public void Close()
    {
        try
        {
            if (_serialPort.IsOpen)
            {
                _serialPort.DiscardInBuffer();
                _serialPort.DiscardOutBuffer();

                _serialPort.Close();
            }
        }
        catch
        {
            // Không để Close làm chết Worker.
        }
    }

    // =========================================================
    // READ REGISTERS
    // =========================================================

    public ushort[] ReadRegisters(
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort quantity
    )
    {
        ValidateReadArguments(functionCode, quantity);

        EnsureOpened();

        byte[] request = BuildReadRequest(slaveId, functionCode, startAddress, quantity);

        Exception? lastException = null;

        for (int attempt = 0; attempt <= MaxRetries; attempt++)
        {
            try
            {
                PrepareForRequest();

                LogFrame("TX", request);

                _serialPort.Write(request, 0, request.Length);

                byte[] response = ReadReadResponse(slaveId, functionCode, quantity);

                LogFrame("RX", response);

                ValidateReadResponse(response, slaveId, functionCode, quantity);

                return ParseRegisters(response, quantity);
            }
            catch (IOException ex)
            {
                lastException = ex;

                if (attempt < MaxRetries)
                {
                    DelayRetry();
                    continue;
                }

                throw;
            }
            catch (TimeoutException ex)
            {
                lastException = ex;

                if (attempt < MaxRetries)
                {
                    DelayRetry();
                    continue;
                }

                throw;
            }
        }

        throw new IOException("Không thể đọc Modbus RTU.", lastException);
    }

    // WRITE MULTIPLE REGISTERS
    public void WriteRegisters(
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort[] values
    )
    {
        ValidateWriteArguments(functionCode, values);

        EnsureOpened();

        ushort quantity = (ushort)values.Length;

        byte[] request = BuildWriteRequest(slaveId, functionCode, startAddress, values);

        Exception? lastException = null;

        for (int attempt = 0; attempt <= MaxRetries; attempt++)
        {
            try
            {
                PrepareForRequest();

                Console.WriteLine(
                    $"Modbus RTU WRITE | "
                        + $"Slave={slaveId} "
                        + $"Function={functionCode} "
                        + $"Address=0x{startAddress:X4} "
                        + $"Quantity={quantity}"
                );

                LogFrame("TX", request);

                _serialPort.Write(request, 0, request.Length);

                byte[] response = ReadWriteResponse(slaveId, functionCode);

                LogFrame("RX", response);

                ValidateWriteResponse(response, slaveId, functionCode, startAddress, quantity);

                Console.WriteLine(
                    $"Modbus RTU WRITE OK | "
                        + $"Slave={slaveId} "
                        + $"Address=0x{startAddress:X4} "
                        + $"Quantity={quantity}"
                );

                return;
            }
            catch (IOException ex)
            {
                lastException = ex;

                Console.WriteLine(
                    $"[Modbus RTU WRITE ERROR] "
                        + $"Attempt={attempt + 1}/{MaxRetries + 1} | "
                        + $"{ex.Message}"
                );

                if (attempt < MaxRetries)
                {
                    DelayRetry();
                    continue;
                }

                throw;
            }
            catch (TimeoutException ex)
            {
                lastException = ex;

                Console.WriteLine(
                    $"[Modbus RTU WRITE TIMEOUT] " + $"Attempt={attempt + 1}/{MaxRetries + 1}"
                );

                if (attempt < MaxRetries)
                {
                    DelayRetry();
                    continue;
                }

                throw;
            }
        }

        throw new IOException("Không thể ghi Modbus RTU.", lastException);
    }

    // PREPARE REQUEST
    private void PrepareForRequest()
    {
        EnsureOpened();

        // Chỉ xóa dữ liệu cũ TRƯỚC khi gửi request.
        if (_serialPort.BytesToRead > 0)
        {
            Console.WriteLine(
                $"[Modbus RTU] Discard stale RX bytes: " + $"{_serialPort.BytesToRead}"
            );

            _serialPort.DiscardInBuffer();
        }

        Thread.Sleep(InterFrameDelayMilliseconds);
    }

    // RETRY
    private static void DelayRetry()
    {
        Thread.Sleep(RetryDelayMilliseconds);
    }

    // VALIDATE READ ARGUMENTS
    private static void ValidateReadArguments(byte functionCode, ushort quantity)
    {
        if (functionCode != 3 && functionCode != 4)
        {
            throw new NotSupportedException(
                $"Modbus RTU chưa hỗ trợ Function Code {functionCode}."
            );
        }

        if (quantity == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(quantity), "Quantity phải lớn hơn 0.");
        }

        if (quantity > MaxReadRegisters)
        {
            throw new ArgumentOutOfRangeException(
                nameof(quantity),
                $"Quantity đọc tối đa là " + $"{MaxReadRegisters} registers."
            );
        }
    }

    // VALIDATE WRITE ARGUMENTS
    private static void ValidateWriteArguments(byte functionCode, ushort[] values)
    {
        if (functionCode != 16)
        {
            throw new NotSupportedException(
                $"Modbus RTU chỉ hỗ trợ ghi bằng " + $"Function Code 16. Nhận {functionCode}."
            );
        }

        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        if (values.Length == 0)
        {
            throw new ArgumentException("Không có giá trị để ghi.", nameof(values));
        }

        if (values.Length > MaxWriteRegisters)
        {
            throw new ArgumentOutOfRangeException(
                nameof(values),
                $"Function Code 16 chỉ cho phép tối đa " + $"{MaxWriteRegisters} registers."
            );
        }
    }

    // ENSURE OPEN
    private void EnsureOpened()
    {
        if (!_serialPort.IsOpen)
        {
            throw new InvalidOperationException("Serial port chưa được mở.");
        }
    }

    // BUILD READ REQUEST
    private static byte[] BuildReadRequest(
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort quantity
    )
    {
        byte[] request = new byte[8];

        request[0] = slaveId;
        request[1] = functionCode;

        request[2] = (byte)(startAddress >> 8);
        request[3] = (byte)(startAddress & 0xFF);

        request[4] = (byte)(quantity >> 8);
        request[5] = (byte)(quantity & 0xFF);

        ushort crc = CalculateCrc(request, 6);

        // Modbus RTU:
        // CRC Low Byte trước
        // CRC High Byte sau

        request[6] = (byte)(crc & 0xFF);
        request[7] = (byte)(crc >> 8);

        return request;
    }

    // BUILD WRITE REQUEST
    // FUNCTION CODE 16
    private static byte[] BuildWriteRequest(
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort[] values
    )
    {
        ushort quantity = (ushort)values.Length;

        byte byteCount = checked((byte)(quantity * 2));

        byte[] request = new byte[9 + byteCount];

        request[0] = slaveId;
        request[1] = functionCode;

        request[2] = (byte)(startAddress >> 8);
        request[3] = (byte)(startAddress & 0xFF);

        request[4] = (byte)(quantity >> 8);
        request[5] = (byte)(quantity & 0xFF);

        request[6] = byteCount;

        for (int i = 0; i < values.Length; i++)
        {
            int offset = 7 + i * 2;

            request[offset] = (byte)(values[i] >> 8);

            request[offset + 1] = (byte)(values[i] & 0xFF);
        }

        ushort crc = CalculateCrc(request, request.Length - 2);

        request[^2] = (byte)(crc & 0xFF);

        request[^1] = (byte)(crc >> 8);

        return request;
    }

    // READ READ RESPONSE
    private byte[] ReadReadResponse(byte expectedSlaveId, byte expectedFunction, ushort quantity)
    {
        while (true)
        {
            byte slaveId = ReadByte();

            if (slaveId != expectedSlaveId)
            {
                continue;
            }

            byte function = ReadByte();

            // EXCEPTION RESPONSE
            if (function == (byte)(expectedFunction | 0x80))
            {
                byte exceptionCode = ReadByte();

                byte crcLo = ReadByte();
                byte crcHi = ReadByte();

                return new[] { slaveId, function, exceptionCode, crcLo, crcHi };
            }

            // WRONG FUNCTION
            if (function != expectedFunction)
            {
                continue;
            }

            // BYTE COUNT
            byte byteCount = ReadByte();

            int expectedByteCount = checked(quantity * 2);

            if (byteCount != expectedByteCount)
            {
                Console.WriteLine(
                    $"[Modbus RTU] Sai ByteCount. "
                        + $"Expected={expectedByteCount}, "
                        + $"Received={byteCount}"
                );

                continue;
            }

            // DATA + CRC
            byte[] remaining = ReadExact(expectedByteCount + 2);

            byte[] response = new byte[3 + remaining.Length];

            response[0] = slaveId;
            response[1] = function;
            response[2] = byteCount;

            Buffer.BlockCopy(remaining, 0, response, 3, remaining.Length);

            return response;
        }
    }

    // READ WRITE RESPONSE
    // FUNCTION CODE 16
    private byte[] ReadWriteResponse(byte expectedSlaveId, byte expectedFunction)
    {
        while (true)
        {
            byte slaveId = ReadByte();

            // KHÔNG PHẢI SLAVE CẦN TÌM
            if (slaveId != expectedSlaveId)
            {
                continue;
            }

            byte function = ReadByte();

            // EXCEPTION RESPONSE
            if (function == (byte)(expectedFunction | 0x80))
            {
                byte exceptionCode = ReadByte();

                byte crcLo = ReadByte();
                byte crcHi = ReadByte();

                return new[] { slaveId, function, exceptionCode, crcLo, crcHi };
            }

            // FUNCTION KHÁC
            if (function != expectedFunction)
            {
                continue;
            }

            // NORMAL FC16 RESPONSE
            byte[] remaining = ReadExact(6);

            byte[] response = new byte[8];

            response[0] = slaveId;
            response[1] = function;

            Buffer.BlockCopy(remaining, 0, response, 2, 6);

            return response;
        }
    }

    // READ BYTE
    private byte ReadByte()
    {
        try
        {
            int value = _serialPort.ReadByte();

            if (value < 0)
            {
                throw new TimeoutException("Không nhận được byte Modbus RTU.");
            }

            return (byte)value;
        }
        catch (TimeoutException)
        {
            throw new TimeoutException("Timeout khi chờ response Modbus RTU.");
        }
    }

    // READ EXACT
    private byte[] ReadExact(int length)
    {
        byte[] buffer = new byte[length];

        int offset = 0;

        while (offset < length)
        {
            try
            {
                int read = _serialPort.Read(buffer, offset, length - offset);

                if (read <= 0)
                {
                    throw new TimeoutException("Không nhận được dữ liệu Modbus RTU.");
                }

                offset += read;
            }
            catch (TimeoutException)
            {
                throw new TimeoutException("Timeout khi đọc Modbus RTU.");
            }
        }

        return buffer;
    }

    // VALIDATE READ RESPONSE
    private static void ValidateReadResponse(
        byte[] response,
        byte expectedSlaveId,
        byte expectedFunction,
        ushort quantity
    )
    {
        if (response.Length < 5)
        {
            throw new IOException("Response Modbus RTU quá ngắn.");
        }

        if (response[0] != expectedSlaveId)
        {
            throw new IOException(
                $"Slave ID không đúng. "
                    + $"Expected={expectedSlaveId}, "
                    + $"Received={response[0]}."
            );
        }

        byte function = response[1];

        // EXCEPTION
        if (function == (byte)(expectedFunction | 0x80))
        {
            byte exceptionCode = response[2];

            ValidateCrc(response);

            throw new IOException(
                $"Modbus Exception. "
                    + $"Function={expectedFunction}, "
                    + $"ExceptionCode={exceptionCode}."
            );
        }

        if (function != expectedFunction)
        {
            throw new IOException(
                $"Function Code không đúng. "
                    + $"Expected={expectedFunction}, "
                    + $"Received={function}."
            );
        }

        int expectedByteCount = checked(quantity * 2);

        if (response[2] != expectedByteCount)
        {
            throw new IOException(
                $"Byte Count không đúng. "
                    + $"Expected={expectedByteCount}, "
                    + $"Received={response[2]}."
            );
        }

        int expectedLength = 5 + expectedByteCount;

        if (response.Length != expectedLength)
        {
            throw new IOException(
                $"Response length không đúng. "
                    + $"Expected={expectedLength}, "
                    + $"Received={response.Length}."
            );
        }

        ValidateCrc(response);
    }

    // VALIDATE WRITE RESPONSE
    private static void ValidateWriteResponse(
        byte[] response,
        byte expectedSlaveId,
        byte expectedFunction,
        ushort expectedStartAddress,
        ushort expectedQuantity
    )
    {
        // GENERAL LENGTH
        if (response == null || response.Length < 5)
        {
            throw new IOException("Write response Modbus RTU quá ngắn.");
        }

        // SLAVE
        if (response[0] != expectedSlaveId)
        {
            throw new IOException(
                $"Slave ID không đúng khi ghi. "
                    + $"Expected={expectedSlaveId}, "
                    + $"Received={response[0]}."
            );
        }

        // EXCEPTION
        if (response[1] == (byte)(expectedFunction | 0x80))
        {
            if (response.Length != 5)
            {
                throw new IOException(
                    $"Exception response length không đúng. "
                        + $"Expected=5, "
                        + $"Received={response.Length}."
                );
            }

            ValidateCrc(response);

            byte exceptionCode = response[2];

            throw new IOException(
                $"Modbus Exception khi ghi. "
                    + $"Function={expectedFunction}, "
                    + $"ExceptionCode={exceptionCode}."
            );
        }

        // FUNCTION
        if (response[1] != expectedFunction)
        {
            throw new IOException(
                $"Function Code không đúng khi ghi. "
                    + $"Expected={expectedFunction}, "
                    + $"Received={response[1]}."
            );
        }

        // NORMAL FC16 = 8 BYTES
        if (response.Length != 8)
        {
            throw new IOException(
                $"Write response length không đúng. "
                    + $"Expected=8, "
                    + $"Received={response.Length}. "
                    + $"Frame={FormatFrame(response)}"
            );
        }

        // ADDRESS
        ushort returnedStartAddress = (ushort)((response[2] << 8) | response[3]);

        if (returnedStartAddress != expectedStartAddress)
        {
            throw new IOException(
                $"Start Address trả về không đúng. "
                    + $"Expected=0x{expectedStartAddress:X4}, "
                    + $"Received=0x{returnedStartAddress:X4}. "
                    + $"Frame={FormatFrame(response)}"
            );
        }

        // QUANTITY
        ushort returnedQuantity = (ushort)((response[4] << 8) | response[5]);

        if (returnedQuantity != expectedQuantity)
        {
            throw new IOException(
                $"Quantity trả về không đúng. "
                    + $"Expected={expectedQuantity}, "
                    + $"Received={returnedQuantity}. "
                    + $"Frame={FormatFrame(response)}"
            );
        }

        // CRC
        ValidateCrc(response);
    }

    // PARSE REGISTERS
    private static ushort[] ParseRegisters(byte[] response, ushort quantity)
    {
        ushort[] registers = new ushort[quantity];

        for (int i = 0; i < quantity; i++)
        {
            int index = 3 + i * 2;

            registers[i] = (ushort)((response[index] << 8) | response[index + 1]);
        }

        return registers;
    }

    // VALIDATE CRC
    private static void ValidateCrc(byte[] response)
    {
        if (response == null || response.Length < 3)
        {
            throw new IOException("Response quá ngắn để kiểm tra CRC.");
        }

        ushort receivedCrc = (ushort)(response[^2] | (response[^1] << 8));

        ushort calculatedCrc = CalculateCrc(response, response.Length - 2);

        if (receivedCrc != calculatedCrc)
        {
            throw new IOException(
                $"CRC Modbus không đúng. "
                    + $"Received=0x{receivedCrc:X4}, "
                    + $"Calculated=0x{calculatedCrc:X4}, "
                    + $"Frame={FormatFrame(response)}"
            );
        }
    }

    // CRC16 MODBUS
    private static ushort CalculateCrc(byte[] data, int length)
    {
        ushort crc = 0xFFFF;

        for (int i = 0; i < length; i++)
        {
            crc ^= data[i];

            for (int j = 0; j < 8; j++)
            {
                if ((crc & 0x0001) != 0)
                {
                    crc = (ushort)((crc >> 1) ^ 0xA001);
                }
                else
                {
                    crc = (ushort)(crc >> 1);
                }
            }
        }

        return crc;
    }

    // FRAME LOG
    private static void LogFrame(string direction, byte[] frame)
    {
        Console.WriteLine($"Modbus RTU {direction}: " + $"{FormatFrame(frame)}");
    }

    // FORMAT FRAME
    private static string FormatFrame(byte[] frame)
    {
        return string.Join(" ", frame.Select(b => b.ToString("X2")));
    }
}
