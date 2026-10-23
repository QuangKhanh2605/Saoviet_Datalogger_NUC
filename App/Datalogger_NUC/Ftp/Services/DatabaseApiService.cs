using System.Collections.Concurrent;
using System.Text.Json;
using Ftp.Models;
using StackExchange.Redis;

namespace Ftp.Services;

public sealed class DatabaseApiService
{
    private const string FtpStreamKey = "database:stream:ftp";
    private const string Ftp2StreamKey = "database:stream:ftp2";

    private readonly IDatabase _redis;
    private readonly ILogger<DatabaseApiService> _logger;

    // Lưu Stream Entry ID tương ứng với Message ID đã đọc.
    // Dùng riêng cho FTP1 và FTP2 để không xóa nhầm stream.
    private readonly ConcurrentDictionary<long, string> _ftpEntries = new();
    private readonly ConcurrentDictionary<long, string> _ftp2Entries = new();

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    // Cập nhật trạng thái và xóa entry trong cùng một Lua script.
    private const string MarkSentScript = """
        local entryId = ARGV[1]
        local statusKey = ARGV[2]
        local dedupKey = ARGV[3]

        redis.call('SET', statusKey, '1')
        local deleted = redis.call('XDEL', KEYS[1], entryId)
        redis.call('DEL', dedupKey)

        return deleted
        """;

    public DatabaseApiService(IConnectionMultiplexer redis, ILogger<DatabaseApiService> logger)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentNullException.ThrowIfNull(logger);

        _redis = redis.GetDatabase();
        _logger = logger;
    }

    // ============================================================
    // FTP1 - GET PENDING FROM REDIS STREAM
    // ============================================================

    public Task<List<StationMessage>> GetPendingFtpMessagesAsync(
        IEnumerable<string>? stationNames,
        int maxMessages,
        CancellationToken cancellationToken
    )
    {
        return GetPendingMessagesAsync(
            FtpStreamKey,
            stationNames,
            maxMessages,
            _ftpEntries,
            cancellationToken
        );
    }

    // ============================================================
    // FTP2 - GET PENDING FROM REDIS STREAM
    // ============================================================

    public Task<List<StationMessage>> GetPendingFtp2MessagesAsync(
        IEnumerable<string>? stationNames,
        int maxMessages,
        CancellationToken cancellationToken
    )
    {
        return GetPendingMessagesAsync(
            Ftp2StreamKey,
            stationNames,
            maxMessages,
            _ftp2Entries,
            cancellationToken
        );
    }

    // ============================================================
    // GET PENDING
    // ============================================================

    private async Task<List<StationMessage>> GetPendingMessagesAsync(
        string streamKey,
        IEnumerable<string>? stationNames,
        int maxMessages,
        ConcurrentDictionary<long, string> entryMap,
        CancellationToken cancellationToken
    )
    {
        var messages = new List<StationMessage>();

        if (maxMessages <= 0)
            return messages;

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            // Đọc tối đa 100 entry từ Redis Stream.
            StreamEntry[] entries = await _redis.StreamRangeAsync(
                streamKey,
                minId: "-",
                maxId: "+",
                count: 100
            );

            cancellationToken.ThrowIfCancellationRequested();

            HashSet<string>? stationFilter = stationNames
                ?.Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (StreamEntry entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (messages.Count >= maxMessages)
                    break;

                NameValueEntry? payloadEntry = entry
                    .Values.Cast<NameValueEntry?>()
                    .FirstOrDefault(x => x.HasValue && x.Value.Name == "payload");

                if (!payloadEntry.HasValue || payloadEntry.Value.Value.IsNullOrEmpty)
                {
                    _logger.LogWarning(
                        "Redis stream entry không có payload. Stream={Stream}, EntryId={EntryId}",
                        streamKey,
                        entry.Id
                    );

                    continue;
                }

                StationMessage? message;

                try
                {
                    message = JsonSerializer.Deserialize<StationMessage>(
                        payloadEntry.Value.Value.ToString(),
                        JsonOptions
                    );
                }
                catch (JsonException ex)
                {
                    _logger.LogError(
                        ex,
                        "Không deserialize được message từ Redis. Stream={Stream}, EntryId={EntryId}",
                        streamKey,
                        entry.Id
                    );

                    continue;
                }

                if (message == null)
                {
                    _logger.LogWarning(
                        "Payload deserialize thành null. Stream={Stream}, EntryId={EntryId}",
                        streamKey,
                        entry.Id
                    );

                    continue;
                }

                if (
                    stationFilter is { Count: > 0 }
                    && !stationFilter.Contains(message.StationName ?? string.Empty)
                )
                {
                    continue;
                }

                // Lưu ánh xạ để xóa đúng entry sau khi upload thành công.
                entryMap[message.Id] = entry.Id.ToString();

                messages.Add(message);
            }

            return messages;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisException ex)
        {
            // Không giả vờ rằng Redis không có bản tin.
            // Ném lỗi để FtpWorker ghi log và thử lại ở vòng sau.
            _logger.LogWarning(
                ex,
                "Không đọc được Redis Stream. Stream={Stream}. Sẽ thử lại ở vòng tiếp theo.",
                streamKey
            );

            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Lỗi khi xử lý dữ liệu pending từ Redis. Stream={Stream}",
                streamKey
            );

            throw;
        }
    }

    // ============================================================
    // FTP1 - MARK SENT
    // ============================================================

    public Task MarkFtpSentAsync(IEnumerable<long> messageIds, CancellationToken cancellationToken)
    {
        return MarkSentAsync(FtpStreamKey, "ftp", messageIds, _ftpEntries, cancellationToken);
    }

    // ============================================================
    // FTP2 - MARK SENT
    // ============================================================

    public Task MarkFtp2SentAsync(IEnumerable<long> messageIds, CancellationToken cancellationToken)
    {
        return MarkSentAsync(Ftp2StreamKey, "ftp2", messageIds, _ftp2Entries, cancellationToken);
    }

    // ============================================================
    // MARK SENT
    // ============================================================

    private async Task MarkSentAsync(
        string streamKey,
        string destination,
        IEnumerable<long> messageIds,
        ConcurrentDictionary<long, string> entryMap,
        CancellationToken cancellationToken
    )
    {
        List<long> ids = messageIds.Distinct().ToList();

        if (ids.Count == 0)
            return;

        foreach (long messageId in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (
                !entryMap.TryGetValue(messageId, out string? entryId)
                || string.IsNullOrWhiteSpace(entryId)
            )
            {
                _logger.LogWarning(
                    "Không tìm thấy Stream Entry ID để mark-sent. Destination={Destination}, MessageId={MessageId}",
                    destination,
                    messageId
                );

                continue;
            }

            string statusKey = $"database:status:{destination}:{messageId}";

            string dedupKey = $"database:dedup:{destination}:{messageId}";

            try
            {
                RedisResult result = await _redis.ScriptEvaluateAsync(
                    MarkSentScript,
                    new RedisKey[] { streamKey },
                    new RedisValue[] { entryId, statusKey, dedupKey }
                );

                cancellationToken.ThrowIfCancellationRequested();

                long deletedCount = (long)result;

                entryMap.TryRemove(messageId, out _);

                if (deletedCount > 0)
                {
                    _logger.LogInformation(
                        "Đã mark-sent và xóa Redis stream entry. Destination={Destination}, MessageId={MessageId}",
                        destination,
                        messageId
                    );
                }
                else
                {
                    _logger.LogWarning(
                        "Đã cập nhật trạng thái nhưng stream entry không còn tồn tại. Destination={Destination}, MessageId={MessageId}, EntryId={EntryId}",
                        destination,
                        messageId,
                        entryId
                    );
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (RedisException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Redis lỗi khi mark-sent. Destination={Destination}, MessageId={MessageId}. Sẽ thử lại ở vòng xử lý tiếp theo nếu entry còn tồn tại.",
                    destination,
                    messageId
                );

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Mark-sent thất bại. Destination={Destination}, MessageId={MessageId}",
                    destination,
                    messageId
                );

                throw;
            }
        }
    }
}
