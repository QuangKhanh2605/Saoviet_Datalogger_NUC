namespace Modbus.Decoders;

public static class RegisterEncoder
{
    public static ushort[] Encode(string type, double value, string byteOrder)
    {
        if (string.IsNullOrWhiteSpace(type))
        {
            throw new ArgumentException("Data type không được rỗng.", nameof(type));
        }

        string normalizedType = type.Trim().ToUpperInvariant();

        string normalizedOrder = string.IsNullOrWhiteSpace(byteOrder)
            ? "ABCD"
            : byteOrder.Trim().ToUpperInvariant();

        return normalizedType switch
        {
            "UINT16" => new[] { checked((ushort)value) },

            "INT16" => new[] { unchecked((ushort)checked((short)value)) },

            "UINT32" => EncodeUInt32(checked((uint)value), normalizedOrder),

            "INT32" => EncodeUInt32(unchecked((uint)checked((int)value)), normalizedOrder),

            "FLOAT32" => EncodeFloat32(checked((float)value), normalizedOrder),

            _ => throw new NotSupportedException($"Kiểu dữ liệu ghi không hỗ trợ: {type}"),
        };
    }

    private static ushort[] EncodeUInt32(uint value, string byteOrder)
    {
        byte a = (byte)(value >> 24);
        byte b = (byte)(value >> 16);
        byte c = (byte)(value >> 8);
        byte d = (byte)value;

        return byteOrder switch
        {
            "ABCD" => new[] { (ushort)((a << 8) | b), (ushort)((c << 8) | d) },

            "CDAB" => new[] { (ushort)((c << 8) | d), (ushort)((a << 8) | b) },

            "BADC" => new[] { (ushort)((b << 8) | a), (ushort)((d << 8) | c) },

            "DCBA" => new[] { (ushort)((d << 8) | c), (ushort)((b << 8) | a) },

            _ => throw new NotSupportedException($"ByteOrder ghi chưa hỗ trợ: {byteOrder}"),
        };
    }

    private static ushort[] EncodeFloat32(float value, string byteOrder)
    {
        uint raw = BitConverter.ToUInt32(BitConverter.GetBytes(value), 0);

        return EncodeUInt32(raw, byteOrder);
    }
}
