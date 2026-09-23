namespace Modbus.Sensors;

public static class StatusCode
{
    public const int Ok = 0;

    public const int Calibrating = 1;

    public const int Error = 2;
}

public static class StatusRuleEvaluator
{
    /// <summary>
    /// Đánh giá status khi parameter đã đọc thành công.
    ///
    /// Có rule:
    ///     rawValue match rule -> status theo rule
    ///     không match          -> fallback "*"
    ///     không có fallback    -> Error
    ///
    /// Không có rule:
    ///     Connected/Read OK -> OK
    /// </summary>
    public static int Evaluate(int rawValue, Dictionary<string, string>? rules)
    {
        // Không có rule:
        // parameter đã đọc thành công
        // nên mặc định OK.
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

    /// <summary>
    /// Đánh giá status có xét connection.
    ///
    /// Connection false:
    ///     ERROR.
    ///
    /// Connection true:
    ///     đánh giá Rule.
    ///
    /// Hàm này được giữ để các thành phần khác
    /// có thể sử dụng nếu cần.
    /// </summary>
    public static int EvaluateConnection(
        bool connected,
        int rawValue,
        Dictionary<string, string>? rules
    )
    {
        if (!connected)
        {
            return StatusCode.Error;
        }

        return Evaluate(rawValue, rules);
    }

    /// <summary>
    /// Kết hợp nhiều status.
    ///
    /// ERROR > CALIBRATING > OK
    /// </summary>
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
