namespace Modbus.Hardware;

public interface IModbusClient
{
    void Open();

    void Close();

    ushort[] ReadRegisters(byte slaveId, byte functionCode, ushort startAddress, ushort quantity);

    void WriteRegisters(byte slaveId, byte functionCode, ushort startAddress, ushort[] values);
}
