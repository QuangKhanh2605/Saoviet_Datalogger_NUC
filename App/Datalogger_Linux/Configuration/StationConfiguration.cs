using Datalogger_Linux.Sensors;

namespace Datalogger_Linux.Configuration;

public class StationConfiguration
{
    public List<StationDefinition> Stations { get; set; } = [];
}
