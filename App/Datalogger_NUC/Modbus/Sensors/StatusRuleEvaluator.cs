namespace Modbus.Sensors;

public static class StatusCode
{
    public const int Ok = 0;

    public const int Calibrating = 1;

    public const int Error = 2;
}

public static class StatusRuleEvaluator
{
    public static int Evaluate(int rawValue, Dictionary<string, string>? rules)
    {
        if (rules == null || rules.Count == 0)
        {
            return StatusCode.Ok;
        }

        string label;

        if (rules.TryGetValue(rawValue.ToString(), out string? matched))
        {
            label = matched;
        }
        else if (rules.TryGetValue("*", out string? fallback))
        {
            label = fallback;
        }
        else
        {
            return StatusCode.Error;
        }

        return label.Trim().ToUpperInvariant() switch
        {
            "OK" => StatusCode.Ok,

            "CALIBRATING" => StatusCode.Calibrating,

            "ERROR" => StatusCode.Error,

            _ => StatusCode.Error,
        };
    }

    public static int Combine(IEnumerable<int> statusCodes)
    {
        bool anyError = false;

        bool anyCalibrating = false;

        foreach (int code in statusCodes)
        {
            if (code == StatusCode.Error)
            {
                anyError = true;
            }
            else if (code == StatusCode.Calibrating)
            {
                anyCalibrating = true;
            }
        }

        if (anyError)
        {
            return StatusCode.Error;
        }

        if (anyCalibrating)
        {
            return StatusCode.Calibrating;
        }

        return StatusCode.Ok;
    }
}
