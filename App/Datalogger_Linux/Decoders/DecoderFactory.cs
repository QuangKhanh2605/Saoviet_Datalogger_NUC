namespace Datalogger_Linux.Decoders;

public class DecoderFactory
{
    // DECODE VALUE
    public double Decode(string type, ushort[] registers, string byteOrder)
    {
        return Decode(type, registers, byteOrder, "NORMAL");
    }

    // DECODE VALUE + BIT ORDER
    public double Decode(string type, ushort[] registers, string byteOrder, string? bitOrder)
    {
        if (string.IsNullOrWhiteSpace(type))
        {
            throw new ArgumentException("Data type không được rỗng.", nameof(type));
        }

        if (registers == null || registers.Length == 0)
        {
            throw new ArgumentException("Không có register để decode.", nameof(registers));
        }

        string normalizedType = NormalizeType(type);

        string normalizedByteOrder = NormalizeByteOrder(byteOrder);

        string normalizedBitOrder = NormalizeBitOrder(bitOrder);

        // Decode bình thường
        double value = DecodeNormal(normalizedType, registers, normalizedByteOrder);

        // Không đảo bit
        if (normalizedBitOrder == "NORMAL")
        {
            return value;
        }

        // ------------------------------------------------------------
        // Đảo bit
        //
        // Decode lại từ raw sau khi đảo bit.
        // ------------------------------------------------------------

        int bitCount = GetTypeBitCount(normalizedType);

        byte[] bytes = GetRequiredBytes(registers, bitCount, normalizedByteOrder);

        ulong raw = BytesToUInt64(bytes);

        raw = ReverseBits(raw, bitCount);

        return DecodeRawUnsigned(normalizedType, raw);
    }

    // DECODE BIT FIELD
    public double DecodeBits(
        ushort[] registers,
        int bitPosition,
        int bitLength,
        string byteOrder = "ABCD",
        string bitOrder = "NORMAL"
    )
    {
        if (registers == null || registers.Length == 0)
        {
            throw new ArgumentException("Không có register để decode.", nameof(registers));
        }

        if (bitPosition < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bitPosition), "BitPosition phải >= 0.");
        }

        if (bitLength <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bitLength), "BitLength phải > 0.");
        }

        if (bitLength > 64)
        {
            throw new ArgumentOutOfRangeException(
                nameof(bitLength),
                "BitLength không được lớn hơn 64."
            );
        }

        int totalBits = registers.Length * 16;

        if (bitPosition + bitLength > totalBits)
        {
            throw new ArgumentException(
                $"Bit field vượt quá dữ liệu. "
                    + $"BitPosition={bitPosition}, "
                    + $"BitLength={bitLength}, "
                    + $"TotalBits={totalBits}."
            );
        }

        string normalizedByteOrder = NormalizeByteOrder(byteOrder);

        string normalizedBitOrder = NormalizeBitOrder(bitOrder);

        // Chuyển register -> byte
        byte[] bytes = RegistersToBytes(registers, normalizedByteOrder);

        // Byte -> UInt64
        ulong raw = BytesToUInt64(bytes);

        // Đảo bit nếu yêu cầu
        if (normalizedBitOrder == "REVERSE")
        {
            int availableBits = Math.Min(registers.Length * 16, 64);

            raw = ReverseBits(raw, availableBits);
        }

        // Extract bit field
        ulong result = ExtractBits(raw, bitPosition, bitLength);

        return result;
    }

    // NORMAL DECODE
    private static double DecodeNormal(string type, ushort[] registers, string byteOrder)
    {
        return type switch
        {
            // 8 BIT
            "UINT8" => DecodeUInt8(registers, byteOrder),

            "INT8" => DecodeInt8(registers, byteOrder),

            // 16 BIT
            "UINT16" => DecodeUInt16(registers, byteOrder),

            "INT16" => DecodeInt16(registers, byteOrder),

            // 32 BIT
            "UINT32" => DecodeUInt32(registers, byteOrder),

            "INT32" => DecodeInt32(registers, byteOrder),

            "FLOAT32" or "FLOAT" => DecodeFloat32(registers, byteOrder),

            // 64 BIT
            "UINT64" => DecodeUInt64(registers, byteOrder),

            "INT64" => DecodeInt64(registers, byteOrder),

            "FLOAT64" or "DOUBLE" => DecodeFloat64(registers, byteOrder),

            // BOOL

            "BOOL" or "BOOLEAN" => DecodeBool(registers),

            _ => throw new NotSupportedException($"Kiểu dữ liệu không hỗ trợ: {type}"),
        };
    }

    // UINT8
    private static double DecodeUInt8(ushort[] registers, string byteOrder)
    {
        byte value = ExtractSingleByte(registers, byteOrder);

        return value;
    }

    // INT8
    private static double DecodeInt8(ushort[] registers, string byteOrder)
    {
        byte raw = ExtractSingleByte(registers, byteOrder);

        return (sbyte)raw;
    }

    // UINT16
    private static double DecodeUInt16(ushort[] registers, string byteOrder)
    {
        if (registers.Length < 1)
        {
            throw new ArgumentException("UINT16 cần 1 register.");
        }

        ushort value = registers[0];

        string order = Normalize16BitOrder(byteOrder);

        return order switch
        {
            "AB" => value,

            "BA" => SwapUInt16Bytes(value),

            _ => throw new NotSupportedException($"ByteOrder không hợp lệ cho UINT16: {byteOrder}"),
        };
    }

    // INT16
    private static double DecodeInt16(ushort[] registers, string byteOrder)
    {
        if (registers.Length < 1)
        {
            throw new ArgumentException("INT16 cần 1 register.");
        }

        ushort raw = registers[0];

        string order = Normalize16BitOrder(byteOrder);

        if (order == "BA")
        {
            raw = SwapUInt16Bytes(raw);
        }

        return (short)raw;
    }

    // UINT32
    private static double DecodeUInt32(ushort[] registers, string byteOrder)
    {
        byte[] bytes = GetRequiredBytes(registers, 32, byteOrder);

        return BytesToUInt32(bytes);
    }

    // INT32
    private static double DecodeInt32(ushort[] registers, string byteOrder)
    {
        byte[] bytes = GetRequiredBytes(registers, 32, byteOrder);

        uint raw = BytesToUInt32(bytes);

        return unchecked((int)raw);
    }

    // FLOAT32

    private static double DecodeFloat32(ushort[] registers, string byteOrder)
    {
        byte[] bytes = GetRequiredBytes(registers, 32, byteOrder);

        uint raw = BytesToUInt32(bytes);

        return BitConverter.UInt32BitsToSingle(raw);
    }

    // UINT64
    private static double DecodeUInt64(ushort[] registers, string byteOrder)
    {
        byte[] bytes = GetRequiredBytes(registers, 64, byteOrder);

        return BytesToUInt64(bytes);
    }

    // INT64
    private static double DecodeInt64(ushort[] registers, string byteOrder)
    {
        byte[] bytes = GetRequiredBytes(registers, 64, byteOrder);

        ulong raw = BytesToUInt64(bytes);

        return unchecked((long)raw);
    }

    // DOUBLE / FLOAT64
    private static double DecodeFloat64(ushort[] registers, string byteOrder)
    {
        byte[] bytes = GetRequiredBytes(registers, 64, byteOrder);

        ulong raw = BytesToUInt64(bytes);

        return BitConverter.UInt64BitsToDouble(raw);
    }

    // BOOL
    private static double DecodeBool(ushort[] registers)
    {
        return registers[0] != 0 ? 1.0 : 0.0;
    }

    // REGISTERS -> BYTES
    private static byte[] RegistersToBytes(ushort[] registers, string byteOrder)
    {
        if (registers.Length == 0)
        {
            throw new ArgumentException("Không có register.");
        }

        List<byte> bytes = new();

        foreach (ushort register in registers)
        {
            bytes.Add((byte)(register >> 8));

            bytes.Add((byte)(register & 0xFF));
        }

        return ApplyByteOrder(bytes.ToArray(), byteOrder);
    }

    // BYTE ORDER
    private static byte[] ApplyByteOrder(byte[] source, string byteOrder)
    {
        if (source.Length == 2)
        {
            return byteOrder switch
            {
                "AB" or "ABCD" => new[] { source[0], source[1] },

                "BA" or "BADC" => new[] { source[1], source[0] },

                _ => throw new NotSupportedException(
                    $"ByteOrder không hỗ trợ cho 16-bit: {byteOrder}"
                ),
            };
        }

        if (source.Length == 4)
        {
            byte a = source[0];
            byte b = source[1];
            byte c = source[2];
            byte d = source[3];

            return byteOrder switch
            {
                "ABCD" => new[] { a, b, c, d },

                "BADC" => new[] { b, a, d, c },

                "CDAB" => new[] { c, d, a, b },

                "DCBA" => new[] { d, c, b, a },

                _ => throw new NotSupportedException($"ByteOrder chưa hỗ trợ: {byteOrder}"),
            };
        }

        if (source.Length == 8)
        {
            byte a = source[0];
            byte b = source[1];
            byte c = source[2];
            byte d = source[3];
            byte e = source[4];
            byte f = source[5];
            byte g = source[6];
            byte h = source[7];

            return byteOrder switch
            {
                "ABCDEFGH" => new[] { a, b, c, d, e, f, g, h },

                "BADCFEHG" => new[] { b, a, d, c, f, e, h, g },

                "EFGHABCD" => new[] { e, f, g, h, a, b, c, d },

                "HGFEDCBA" => new[] { h, g, f, e, d, c, b, a },

                // Alias:
                "ABCD" => new[] { a, b, c, d, e, f, g, h },

                "DCBA" => new[] { h, g, f, e, d, c, b, a },

                _ => throw new NotSupportedException(
                    $"ByteOrder chưa hỗ trợ cho 64-bit: {byteOrder}"
                ),
            };
        }

        throw new NotSupportedException($"Không hỗ trợ dữ liệu {source.Length} byte.");
    }

    // REQUIRED BYTES
    private static byte[] GetRequiredBytes(ushort[] registers, int bitCount, string byteOrder)
    {
        int requiredBytes = bitCount / 8;

        int requiredRegisters = (requiredBytes + 1) / 2;

        if (registers.Length < requiredRegisters)
        {
            throw new ArgumentException(
                $"Kiểu dữ liệu {bitCount}-bit cần "
                    + $"{requiredRegisters} registers, "
                    + $"nhưng chỉ có {registers.Length}."
            );
        }

        ushort[] selectedRegisters = registers.Take(requiredRegisters).ToArray();

        byte[] bytes = RegistersToBytes(selectedRegisters, byteOrder);

        return bytes.Take(requiredBytes).ToArray();
    }

    // UINT8 / INT8
    private static byte ExtractSingleByte(ushort[] registers, string byteOrder)
    {
        ushort register = registers[0];

        byte high = (byte)(register >> 8);

        byte low = (byte)(register & 0xFF);

        return byteOrder switch
        {
            "AB" => high,

            "BA" => low,

            "ABCD" => high,

            "BADC" => low,

            "CDAB" => registers.Length >= 2
                ? (byte)(registers[1] >> 8)
                : throw new ArgumentException("CDAB cần ít nhất 2 registers."),

            "DCBA" => registers.Length >= 2
                ? (byte)(registers[1] & 0xFF)
                : throw new ArgumentException("DCBA cần ít nhất 2 registers."),

            _ => throw new NotSupportedException(
                $"ByteOrder không hỗ trợ cho UInt8/Int8: {byteOrder}"
            ),
        };
    }

    // 16 BIT BYTE ORDER
    private static string Normalize16BitOrder(string byteOrder)
    {
        return byteOrder switch
        {
            "AB" => "AB",

            "BA" => "BA",

            "ABCD" => "AB",

            "BADC" => "BA",

            _ => throw new NotSupportedException($"ByteOrder không hợp lệ cho 16-bit: {byteOrder}"),
        };
    }

    // SWAP UINT16
    private static ushort SwapUInt16Bytes(ushort value)
    {
        return (ushort)(((value & 0x00FF) << 8) | ((value & 0xFF00) >> 8));
    }

    // BYTES -> UINT32
    private static uint BytesToUInt32(byte[] bytes)
    {
        if (bytes.Length != 4)
        {
            throw new ArgumentException("UInt32 cần đúng 4 byte.");
        }

        return ((uint)bytes[0] << 24) | ((uint)bytes[1] << 16) | ((uint)bytes[2] << 8) | bytes[3];
    }

    // BYTES -> UINT64
    private static ulong BytesToUInt64(byte[] bytes)
    {
        if (bytes.Length > 8)
        {
            throw new ArgumentException("UInt64 tối đa 8 byte.");
        }

        ulong result = 0;

        foreach (byte value in bytes)
        {
            result = (result << 8) | value;
        }

        return result;
    }

    // EXTRACT BIT FIELD
    private static ulong ExtractBits(ulong raw, int bitPosition, int bitLength)
    {
        if (bitLength == 64)
        {
            return raw;
        }

        ulong mask = (1UL << bitLength) - 1UL;

        return (raw >> bitPosition) & mask;
    }

    // REVERSE BITS
    private static ulong ReverseBits(ulong value, int bitCount)
    {
        ulong result = 0;

        for (int i = 0; i < bitCount; i++)
        {
            ulong bit = (value >> i) & 1UL;

            result |= bit << (bitCount - 1 - i);
        }

        return result;
    }

    // TYPE BIT COUNT
    private static int GetTypeBitCount(string type)
    {
        return type switch
        {
            "UINT8" or "INT8" => 8,

            "UINT16" or "INT16" => 16,

            "UINT32" or "INT32" or "FLOAT32" or "FLOAT" => 32,

            "UINT64" or "INT64" or "FLOAT64" or "DOUBLE" => 64,

            "BOOL" or "BOOLEAN" => 1,

            _ => throw new NotSupportedException($"Không xác định được số bit cho type: {type}"),
        };
    }

    // TYPE NORMALIZATION
    private static string NormalizeType(string type)
    {
        return type.Trim().ToUpperInvariant().Replace("_", "").Replace("-", "");
    }

    // BYTE ORDER NORMALIZATION
    private static string NormalizeByteOrder(string? byteOrder)
    {
        if (string.IsNullOrWhiteSpace(byteOrder))
        {
            return "ABCD";
        }

        return byteOrder.Trim().ToUpperInvariant();
    }

    // BIT ORDER NORMALIZATION
    private static string NormalizeBitOrder(string? bitOrder)
    {
        if (string.IsNullOrWhiteSpace(bitOrder))
        {
            return "NORMAL";
        }

        string value = bitOrder.Trim().ToUpperInvariant();

        return value switch
        {
            "NORMAL" => "NORMAL",

            "REVERSE" => "REVERSE",

            "REVERSED" => "REVERSE",

            "LSB" => "NORMAL",

            "MSB" => "REVERSE",

            _ => throw new NotSupportedException($"BitOrder không hỗ trợ: {bitOrder}"),
        };
    }

    // RAW -> TYPE
    private static double DecodeRawUnsigned(string type, ulong raw)
    {
        return type switch
        {
            "UINT8" => (byte)raw,

            "INT8" => (sbyte)(byte)raw,

            "UINT16" => (ushort)raw,

            "INT16" => (short)(ushort)raw,

            "UINT32" => (uint)raw,

            "INT32" => unchecked((int)(uint)raw),

            "FLOAT32" or "FLOAT" => BitConverter.UInt32BitsToSingle((uint)raw),

            "UINT64" => raw,

            "INT64" => unchecked((long)raw),

            "FLOAT64" or "DOUBLE" => BitConverter.UInt64BitsToDouble(raw),

            "BOOL" or "BOOLEAN" => raw != 0 ? 1.0 : 0.0,

            _ => throw new NotSupportedException($"Kiểu dữ liệu không hỗ trợ: {type}"),
        };
    }
}
