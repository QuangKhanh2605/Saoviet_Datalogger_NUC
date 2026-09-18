using System.Net.Sockets;

namespace Datalogger_Linux.Hardware;

public class ModbusTcp : IModbusClient
{
    private const int ConnectTimeoutMilliseconds = 3000;
    private const int ReadTimeoutMilliseconds = 3000;
    private const int WriteTimeoutMilliseconds = 3000;

    private const ushort MaxReadRegisters = 125;
    private const ushort MaxWriteRegisters = 123;

    private readonly string _ipAddress;
    private readonly int _port;

    private TcpClient? _tcpClient;
    private NetworkStream? _stream;

    private ushort _transactionId;

    public ModbusTcp(string ipAddress, int port = 502)
    {
        if (string.IsNullOrWhiteSpace(ipAddress))
        {
            throw new ArgumentException("IP Address không được rỗng.", nameof(ipAddress));
        }

        if (port <= 0 || port > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(port), "Port không hợp lệ.");
        }

        _ipAddress = ipAddress.Trim();
        _port = port;
    }

    // OPEN
    public void Open()
    {
        if (_tcpClient != null && _stream != null && _tcpClient.Connected)
        {
            return;
        }

        Close();

        TcpClient client = new();

        try
        {
            Task connectTask = client.ConnectAsync(_ipAddress, _port);

            if (!connectTask.Wait(ConnectTimeoutMilliseconds))
            {
                client.Close();

                throw new TimeoutException($"Timeout kết nối Modbus TCP {_ipAddress}:{_port}.");
            }

            // Đảm bảo exception từ ConnectAsync được ném ra.
            connectTask.GetAwaiter().GetResult();

            NetworkStream stream = client.GetStream();

            stream.ReadTimeout = ReadTimeoutMilliseconds;

            stream.WriteTimeout = WriteTimeoutMilliseconds;

            _tcpClient = client;
            _stream = stream;

            _transactionId = 0;
        }
        catch
        {
            try
            {
                client.Close();
            }
            catch { }

            throw;
        }
    }

    // CLOSE
    public void Close()
    {
        try
        {
            _stream?.Close();
        }
        catch { }
        finally
        {
            _stream = null;
        }

        try
        {
            _tcpClient?.Close();
        }
        catch { }
        finally
        {
            _tcpClient = null;
        }
    }

    // READ REGISTERS
    public ushort[] ReadRegisters(
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort quantity
    )
    {
        ValidateReadArguments(functionCode, quantity);

        EnsureOpened();

        ushort transactionId = NextTransactionId();

        byte[] request = BuildReadRequest(
            transactionId,
            slaveId,
            functionCode,
            startAddress,
            quantity
        );

        NetworkStream stream = GetStream();

        try
        {
            stream.Write(request, 0, request.Length);

            // -------------------------------------------------
            // MBAP 7 bytes
            // + Function 1 byte
            // + ByteCount 1 byte
            // = 9 bytes
            // -------------------------------------------------

            byte[] header = ReadExact(9);

            ValidateTransactionAndProtocol(header, transactionId, slaveId);

            ushort length = ReadUInt16(header, 4);

            if (length < 2)
            {
                throw new IOException($"Modbus TCP Length không hợp lệ: {length}.");
            }

            byte responseFunction = header[7];

            // EXCEPTION RESPONSE
            if (responseFunction == (byte)(functionCode | 0x80))
            {
                if (length != 3)
                {
                    throw new IOException(
                        $"Modbus TCP Exception Length không đúng. "
                            + $"Expected=3, Received={length}."
                    );
                }

                byte exceptionCode = header[8];

                throw new IOException(
                    $"Modbus TCP Exception. "
                        + $"Function={functionCode}, "
                        + $"ExceptionCode={exceptionCode}."
                );
            }

            // FUNCTION CODE
            if (responseFunction != functionCode)
            {
                throw new IOException(
                    $"Modbus TCP Function Code không đúng. "
                        + $"Expected={functionCode}, "
                        + $"Received={responseFunction}."
                );
            }

            // BYTE COUNT
            byte byteCount = header[8];

            int expectedByteCount = checked(quantity * 2);

            if (byteCount != expectedByteCount)
            {
                throw new IOException(
                    $"Modbus TCP Byte Count không đúng. "
                        + $"Expected={expectedByteCount}, "
                        + $"Received={byteCount}."
                );
            }

            // length =
            // Unit ID       1
            // Function      1
            // Byte Count    1
            // Data          N
            //
            // => 3 + byteCount

            ushort expectedLength = checked((ushort)(3 + byteCount));

            if (length != expectedLength)
            {
                throw new IOException(
                    $"Modbus TCP Length không đúng. "
                        + $"Expected={expectedLength}, "
                        + $"Received={length}."
                );
            }

            byte[] data = ReadExact(byteCount);

            return ParseRegisters(data, quantity);
        }
        catch
        {
            // Connection có thể đã hỏng.
            // Không giữ socket lỗi để lần đọc sau
            // tiếp tục dùng socket cũ.
            Close();

            throw;
        }
    }

    // WRITE MULTIPLE REGISTERS
    // FUNCTION CODE 16
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

        ushort transactionId = NextTransactionId();

        byte[] request = BuildWriteRequest(
            transactionId,
            slaveId,
            functionCode,
            startAddress,
            values
        );

        NetworkStream stream = GetStream();

        try
        {
            stream.Write(request, 0, request.Length);

            // Write Multiple Registers response:
            //
            // Transaction ID 2
            // Protocol ID    2
            // Length         2
            // Unit ID        1
            // Function       1
            // Start Address  2
            // Quantity       2
            //
            // Total = 12 bytes

            byte[] response = ReadExact(12);

            ValidateTransactionAndProtocol(response, transactionId, slaveId);

            ushort length = ReadUInt16(response, 4);

            byte responseFunction = response[7];

            // EXCEPTION
            if (responseFunction == (byte)(functionCode | 0x80))
            {
                if (length != 3)
                {
                    throw new IOException(
                        $"Modbus TCP Write Exception Length không đúng. "
                            + $"Expected=3, Received={length}."
                    );
                }

                byte exceptionCode = response[8];

                throw new IOException(
                    $"Modbus TCP Exception khi ghi. "
                        + $"Function={functionCode}, "
                        + $"ExceptionCode={exceptionCode}."
                );
            }

            // FUNCTION
            if (responseFunction != functionCode)
            {
                throw new IOException(
                    $"Modbus TCP Function Code không đúng khi ghi. "
                        + $"Expected={functionCode}, "
                        + $"Received={responseFunction}."
                );
            }

            // LENGTH
            if (length != 6)
            {
                throw new IOException(
                    $"Modbus TCP Write Length không đúng. " + $"Expected=6, Received={length}."
                );
            }

            // START ADDRESS
            ushort returnedStartAddress = ReadUInt16(response, 8);

            if (returnedStartAddress != startAddress)
            {
                throw new IOException(
                    $"Start Address trả về không đúng khi ghi. "
                        + $"Expected={startAddress}, "
                        + $"Received={returnedStartAddress}."
                );
            }

            // QUANTITY
            ushort returnedQuantity = ReadUInt16(response, 10);

            if (returnedQuantity != quantity)
            {
                throw new IOException(
                    $"Quantity trả về không đúng khi ghi. "
                        + $"Expected={quantity}, "
                        + $"Received={returnedQuantity}."
                );
            }
        }
        catch
        {
            Close();

            throw;
        }
    }

    // VALIDATE READ ARGUMENTS
    private static void ValidateReadArguments(byte functionCode, ushort quantity)
    {
        if (functionCode != 3 && functionCode != 4)
        {
            throw new NotSupportedException(
                $"Modbus TCP chưa hỗ trợ Function Code {functionCode}."
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
                $"Quantity đọc tối đa là {MaxReadRegisters}."
            );
        }
    }

    // VALIDATE WRITE ARGUMENTS
    private static void ValidateWriteArguments(byte functionCode, ushort[] values)
    {
        if (functionCode != 16)
        {
            throw new NotSupportedException(
                $"Modbus TCP chỉ hỗ trợ ghi bằng Function Code 16. " + $"Nhận {functionCode}."
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
                $"Function Code 16 chỉ cho phép tối đa {MaxWriteRegisters} registers."
            );
        }
    }

    // BUILD READ REQUEST
    private static byte[] BuildReadRequest(
        ushort transactionId,
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort quantity
    )
    {
        byte[] request = new byte[12];

        // Transaction ID
        WriteUInt16(request, 0, transactionId);

        // Protocol ID = 0
        WriteUInt16(request, 2, 0);

        // Length = Unit ID + PDU
        // 1 + 1 + 2 + 2 = 6
        WriteUInt16(request, 4, 6);

        // Unit ID
        request[6] = slaveId;

        // Function
        request[7] = functionCode;

        // Start Address
        WriteUInt16(request, 8, startAddress);

        // Quantity
        WriteUInt16(request, 10, quantity);

        return request;
    }

    // BUILD WRITE REQUEST
    private static byte[] BuildWriteRequest(
        ushort transactionId,
        byte slaveId,
        byte functionCode,
        ushort startAddress,
        ushort[] values
    )
    {
        ushort quantity = (ushort)values.Length;

        byte byteCount = checked((byte)(quantity * 2));

        // Unit ID
        // Function
        // Start Address 2
        // Quantity 2
        // Byte Count
        // Data
        //
        // = 7 + byteCount

        ushort length = checked((ushort)(7 + byteCount));

        byte[] request = new byte[6 + length];

        // Transaction ID
        WriteUInt16(request, 0, transactionId);

        // Protocol ID
        WriteUInt16(request, 2, 0);

        // Length
        WriteUInt16(request, 4, length);

        // Unit ID
        request[6] = slaveId;

        // Function
        request[7] = functionCode;

        // Start Address
        WriteUInt16(request, 8, startAddress);

        // Quantity
        WriteUInt16(request, 10, quantity);

        // Byte Count
        request[12] = byteCount;

        // Data
        for (int i = 0; i < values.Length; i++)
        {
            WriteUInt16(request, 13 + i * 2, values[i]);
        }

        return request;
    }

    // VALIDATE MBAP
    private static void ValidateTransactionAndProtocol(
        byte[] response,
        ushort expectedTransactionId,
        byte expectedUnitId
    )
    {
        if (response.Length < 9)
        {
            throw new IOException("Modbus TCP response quá ngắn.");
        }

        // TRANSACTION ID
        ushort transactionId = ReadUInt16(response, 0);

        if (transactionId != expectedTransactionId)
        {
            throw new IOException(
                $"Transaction ID Modbus TCP không đúng. "
                    + $"Expected={expectedTransactionId}, "
                    + $"Received={transactionId}."
            );
        }

        // PROTOCOL ID
        ushort protocolId = ReadUInt16(response, 2);

        if (protocolId != 0)
        {
            throw new IOException($"Protocol ID Modbus TCP không đúng: {protocolId}.");
        }

        // LENGTH
        ushort length = ReadUInt16(response, 4);

        if (length < 2)
        {
            throw new IOException($"MBAP Length không hợp lệ: {length}.");
        }

        // UNIT ID
        if (response[6] != expectedUnitId)
        {
            throw new IOException(
                $"Unit ID Modbus TCP không đúng. "
                    + $"Expected={expectedUnitId}, "
                    + $"Received={response[6]}."
            );
        }
    }

    // PARSE REGISTERS
    private static ushort[] ParseRegisters(byte[] data, ushort quantity)
    {
        int expectedLength = checked(quantity * 2);

        if (data.Length != expectedLength)
        {
            throw new IOException(
                $"Số byte register không đúng. "
                    + $"Expected={expectedLength}, "
                    + $"Received={data.Length}."
            );
        }

        ushort[] registers = new ushort[quantity];

        for (int i = 0; i < quantity; i++)
        {
            int index = i * 2;

            registers[i] = (ushort)((data[index] << 8) | data[index + 1]);
        }

        return registers;
    }

    // READ EXACT
    private byte[] ReadExact(int length)
    {
        if (length <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        NetworkStream stream = GetStream();

        byte[] buffer = new byte[length];

        int offset = 0;

        while (offset < length)
        {
            int read;

            try
            {
                read = stream.Read(buffer, offset, length - offset);
            }
            catch (IOException ex)
            {
                throw new IOException("Lỗi đọc dữ liệu Modbus TCP.", ex);
            }

            if (read <= 0)
            {
                throw new IOException("Modbus TCP connection đã đóng.");
            }

            offset += read;
        }

        return buffer;
    }

    // STREAM
    private NetworkStream GetStream()
    {
        if (_stream == null)
        {
            throw new InvalidOperationException("Modbus TCP chưa được mở.");
        }

        return _stream;
    }

    private void EnsureOpened()
    {
        if (_tcpClient == null || _stream == null || !_tcpClient.Connected)
        {
            throw new InvalidOperationException("Modbus TCP chưa được mở hoặc connection đã mất.");
        }
    }

    // TRANSACTION ID
    private ushort NextTransactionId()
    {
        unchecked
        {
            _transactionId++;

            if (_transactionId == 0)
            {
                _transactionId = 1;
            }

            return _transactionId;
        }
    }

    // BIG-ENDIAN HELPERS
    private static ushort ReadUInt16(byte[] buffer, int offset)
    {
        return (ushort)((buffer[offset] << 8) | buffer[offset + 1]);
    }

    private static void WriteUInt16(byte[] buffer, int offset, ushort value)
    {
        buffer[offset] = (byte)(value >> 8);

        buffer[offset + 1] = (byte)(value & 0xFF);
    }
}
