namespace Database.Models;

public class DatabaseCleanupResult
{
    public long SizeBeforeBytes { get; set; }

    public long SizeAfterBytes { get; set; }

    public int DeletedByRetention { get; set; }

    public int DeletedBySize { get; set; }

    public int TotalDeleted =>
        DeletedByRetention + DeletedBySize;
}