using Modbus.Configuration;

namespace Modbus.Sensors;

// FILE CONFIG CẤP TRẠM
public class StationDefinition
{
    public bool Enabled { get; set; } = true;

    public string StationName { get; set; } = "";

    public ConnectionDefinition Connection { get; set; } = new();

    public List<SlaveDefinition> Sensors { get; set; } = new();
}

// 1 THIẾT BỊ (SLAVE)
public class SlaveDefinition
{
    public byte SlaveId { get; set; }

    public List<RegisterBlockDefinition> Blocks { get; set; } = new();

    public List<WriteDefinition> Write { get; set; } = new();
}

// 1 BLOCK THANH GHI
public class RegisterBlockDefinition
{
    public string StartAddress { get; set; } = "0x0000";

    public ushort Quantity { get; set; }

    public byte Function { get; set; } = 3;

    // CÁC PARAMETER ĐƯỢC ĐỌC TRONG BLOCK
    public List<ParameterDefinition> Parameters { get; set; } = new();

    // CÁC STATUS ĐƯỢC ĐỌC TRONG BLOCK
    //
    // Status không phải là Parameter.
    // Status tham chiếu tới Parameter thông qua StatusDefinition.Parameter.
    //
    // Status có thể:
    // 1. Nằm cùng block với Parameter
    // 2. Nằm ở block khác
    public List<StatusDefinition> Statuses { get; set; } = new();

    public ushort StartAddressValue => HexUtil.ParseUShort(StartAddress);
}

// 1 THÔNG SỐ ĐO
public class ParameterDefinition
{
    public string Name { get; set; } = "";

    public string Unit { get; set; } = "";

    public ReadDefinition Read { get; set; } = new();
}

// DATA DEFINITION CHUNG
public class DataDefinition
{
    // ADDRESS
    public string Address { get; set; } = "0x0000";

    // DATA TYPE
    public string Type { get; set; } = "UInt16";

    // REGISTER LENGTH
    public int Length { get; set; } = 1;

    // BYTE ORDER
    public string ByteOrder { get; set; } = "ABCD";

    // BIT ORDER
    public string BitOrder { get; set; } = "NORMAL";

    // BIT FIELD
    public int? BitPosition { get; set; }

    public int? BitLength { get; set; }

    // PARSE ADDRESS
    public ushort AddressValue => HexUtil.ParseUShort(Address);
}

// READ VALUE
public class ReadDefinition : DataDefinition
{
    // SCALE
    public double Scale { get; set; } = 1.0;

    // OFFSET
    public double OffsetValue { get; set; } = 0.0;
}

// STATUS
public class StatusDefinition : DataDefinition
{
    // PARAMETER MÀ STATUS NÀY THUỘC VỀ
    //
    // Ví dụ:
    // "Parameter": "pH"
    //
    // Đây chỉ là reference tới Parameter,
    // không phải Parameter chứa Status.
    public string Parameter { get; set; } = "";

    // RULES
    public Dictionary<string, string> Rules { get; set; } = new();
}

// WRITE-BACK
public class WriteDefinition
{
    public string Address { get; set; } = "0x0000";

    public byte Function { get; set; } = 16;

    public string Type { get; set; } = "Float32";

    public int Length { get; set; } = 2;

    public string ByteOrder { get; set; } = "ABCD";

    public WriteSource Source { get; set; } = new();

    public double FallbackValue { get; set; } = 0.0;

    public double Scale { get; set; } = 1.0;

    public ushort AddressValue => HexUtil.ParseUShort(Address);
}

// WRITE SOURCE
public class WriteSource
{
    public string Parameter { get; set; } = "";
}
