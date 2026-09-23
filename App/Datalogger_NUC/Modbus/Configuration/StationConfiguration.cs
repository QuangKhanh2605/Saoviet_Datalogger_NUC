using Modbus.Sensors;

namespace Modbus.Configuration;

public class StationConfiguration
{
    public List<StationDefinition> Stations { get; set; } = [];
}
