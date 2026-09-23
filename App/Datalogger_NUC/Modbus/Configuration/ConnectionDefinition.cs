namespace Modbus.Configuration;

public class ConnectionDefinition
{
    public string Protocol { get; set; } = "ModbusRtu";

    public string PortName { get; set; } = "";

    public int BaudRate { get; set; } = 9600;

    public string Parity { get; set; } = "None";

    public int DataBits { get; set; } = 8;

    public string StopBits { get; set; } = "One";

    public string IpAddress { get; set; } = "";

    public int Port { get; set; } = 502;
}
