namespace Datalogger_Linux.Configuration;

public class ConnectionDefinition
{
    // PROTOCOL
    public string Protocol { get; set; } = "ModbusRtu";

    // SERIAL PORT
    public string PortName { get; set; } = "";

    public int BaudRate { get; set; } = 9600;

    public string Parity { get; set; } = "None";

    public int DataBits { get; set; } = 8;

    public string StopBits { get; set; } = "One";

    // TCP
    public string IpAddress { get; set; } = "";

    public int Port { get; set; } = 502;
}
