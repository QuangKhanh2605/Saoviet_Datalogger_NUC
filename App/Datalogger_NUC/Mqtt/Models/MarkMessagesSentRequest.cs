namespace Database.Models;

public class MarkMessagesSentRequest
{
    public List<long> Ids { get; set; } = new();
}
