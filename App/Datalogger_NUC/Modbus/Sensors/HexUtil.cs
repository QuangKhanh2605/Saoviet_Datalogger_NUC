namespace Modbus.Sensors;

public static class HexUtil
{
    // PARSE ĐỊA CHỈ DẠNG "0x0000" HOẶC SỐ THƯỜNG
    public static ushort ParseUShort(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            throw new FormatException("Địa chỉ Modbus rỗng.");
        }

        string trimmed = text.Trim();

        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            trimmed = trimmed.Substring(2);

            return Convert.ToUInt16(trimmed, 16);
        }

        return ushort.Parse(trimmed);
    }
}
