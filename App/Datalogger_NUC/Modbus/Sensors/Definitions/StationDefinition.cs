using Modbus.Configuration;

namespace Modbus.Sensors;

// ============================================================
// STATION
// ============================================================

public class StationDefinition
{
    public bool Enabled { get; set; } = true;

    public string StationName { get; set; } = "";

    public List<StationConnectionDefinition> Connections { get; set; } = new();
}

// ============================================================
// STATION CONNECTION
// ============================================================

public class StationConnectionDefinition
{
    public ConnectionDefinition Connection { get; set; } = new();

    public List<SlaveDefinition> Sensors { get; set; } = new();
}

// ============================================================
// SLAVE
// ============================================================

public class SlaveDefinition
{
    public byte SlaveId { get; set; }

    public List<RegisterBlockDefinition> Blocks { get; set; } = new();

    public List<WriteDefinition> Write { get; set; } = new();
}

// ============================================================
// REGISTER BLOCK
// ============================================================

public class RegisterBlockDefinition
{
    public string StartAddress { get; set; } = "0x0000";

    public ushort Quantity { get; set; }

    public byte Function { get; set; } = 3;

    public List<ParameterDefinition> Parameters { get; set; } = new();

    public List<StatusDefinition> Statuses { get; set; } = new();

    public ushort StartAddressValue => HexUtil.ParseUShort(StartAddress);
}

// ============================================================
// PARAMETER
// ============================================================

public class ParameterDefinition
{
    public string Name { get; set; } = "";

    public string Unit { get; set; } = "";

    public ReadDefinition Read { get; set; } = new();
}

// ============================================================
// DATA DEFINITION CHUNG
// ============================================================

public class DataDefinition
{
    public string Address { get; set; } = "0x0000";

    public string Type { get; set; } = "UInt16";

    public int Length { get; set; } = 1;

    public string ByteOrder { get; set; } = "ABCD";

    public string BitOrder { get; set; } = "NORMAL";

    public int? BitPosition { get; set; }

    public int? BitLength { get; set; }

    public ushort AddressValue => HexUtil.ParseUShort(Address);
}

// ============================================================
// READ VALUE
// ============================================================

public class ReadDefinition : DataDefinition
{
    public double Scale { get; set; } = 1.0;

    public double OffsetValue { get; set; } = 0.0;
}

// ============================================================
// STATUS
// ============================================================

public class StatusDefinition : DataDefinition
{
    public string Parameter { get; set; } = "";

    public Dictionary<string, string> Rules { get; set; } = new();
}

// ============================================================
// WRITE-BACK
// ============================================================

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

// ============================================================
// WRITE SOURCE
// ============================================================

public class WriteSource
{
    public string Parameter { get; set; } = "";
}
