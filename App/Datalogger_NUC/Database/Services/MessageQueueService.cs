using System.Net;
using System.Text.Json;
using Database.Models;
using StackExchange.Redis;

namespace Database.Services;

public sealed class MessageQueueService
{
    private const int MaxQueueSize = 100;
    private const int RecoveryBatchSize = 50;
    private const int ArchiveBatchSize = 100;
    private const int MaxSequence = 999;

    private const string ArchiveStream = "database:stream:archive";
    private const string MqttStream = "database:stream:mqtt";
    private const string FtpStream = "database:stream:ftp";
    private const string Ftp2Stream = "database:stream:ftp2";

    private const string MqttStatusPrefix = "database:status:mqtt:";
    private const string FtpStatusPrefix = "database:status:ftp:";
    private const string Ftp2StatusPrefix = "database:status:ftp2:";

    private static readonly object MessageIdLock = new();
    private static long _lastTimestampMilliseconds = -1;
    private static int _lastSequence = -1;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly IDatabase _redis;
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly DatabaseService _databaseService;
    private readonly ILogger<MessageQueueService> _logger;

    public MessageQueueService(
        IConnectionMultiplexer redis,
        DatabaseService databaseService,
        ILogger<MessageQueueService> logger
    )
    {
        _multiplexer = redis;
        _redis = redis.GetDatabase();
        _databaseService = databaseService;
        _logger = logger;

        redis.ConnectionFailed += (_, args) =>
            _logger.LogWarning(args.Exception, "Redis connection failed.");

        redis.ConnectionRestored += (_, args) =>
            _logger.LogInformation("Redis connection restored: {EndPoint}", args.EndPoint);
    }

    public static string GetArchiveStreamName() => ArchiveStream;

    public static string GetMqttStreamName() => MqttStream;

    public static string GetFtpStreamName() => FtpStream;

    public static string GetFtp2StreamName() => Ftp2Stream;

    private static async Task<long> GenerateMessageIdAsync(
        CancellationToken cancellationToken = default
    )
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (MessageIdLock)
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                if (timestamp > _lastTimestampMilliseconds)
                {
                    _lastTimestampMilliseconds = timestamp;
                    _lastSequence = 0;
                    return checked(timestamp * 1000L);
                }

                if (_lastSequence < MaxSequence)
                    return checked(_lastTimestampMilliseconds * 1000L + ++_lastSequence);
            }

            await Task.Delay(1, cancellationToken);
        }
    }

    public int EnqueueStationMessages(IEnumerable<StationMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);

        int count = 0;

        foreach (StationMessage message in messages)
        {
            if (
                message is null
                || string.IsNullOrWhiteSpace(message.StationName)
                || string.IsNullOrWhiteSpace(message.Timestamp)
            )
            {
                _logger.LogWarning("Skipping invalid station message.");
                continue;
            }

            EnqueueStationMessageAsync(message).GetAwaiter().GetResult();
            count++;
        }

        return count;
    }

    public async Task EnqueueStationMessageAsync(
        StationMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);
        ValidateMessage(message);
        cancellationToken.ThrowIfCancellationRequested();

        if (message.Id <= 0)
            message.Id = await GenerateMessageIdAsync(cancellationToken);

        RedisResult result = await _redis.ScriptEvaluateAsync(
            EnqueueNewMessageScript,
            new RedisKey[] { ArchiveStream, MqttStream, FtpStream, Ftp2Stream },
            new RedisValue[]
            {
                message.Id,
                JsonSerializer.Serialize(message, JsonOptions),
                MaxQueueSize,
                message.MqttSent ? 1 : 0,
                message.FtpSent ? 1 : 0,
                message.Ftp2Sent ? 1 : 0,
            }
        );

        if ((long)result == 0)
            _logger.LogDebug("Message {MessageId} already exists in Archive.", message.Id);
        else
            _logger.LogDebug("Message {MessageId} enqueued.", message.Id);
    }

    private static void ValidateMessage(StationMessage message)
    {
        if (message.Id < 0)
            throw new ArgumentException("Message Id cannot be negative.", nameof(message));

        if (string.IsNullOrWhiteSpace(message.StationName))
            throw new ArgumentException("StationName cannot be empty.", nameof(message));

        if (string.IsNullOrWhiteSpace(message.Timestamp))
            throw new ArgumentException("Timestamp cannot be empty.", nameof(message));
    }

    public async Task SynchronizeAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            StreamEntry[] entries = await _redis.StreamRangeAsync(
                ArchiveStream,
                minId: "-",
                maxId: "+",
                count: ArchiveBatchSize,
                messageOrder: Order.Ascending
            );

            if (entries.Length == 0)
                break;

            List<StationMessage> messages = new(entries.Length);
            List<bool[]> observedStatuses = new(entries.Length);

            foreach (StreamEntry entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string? payload = GetField(entry, "payload");

                if (string.IsNullOrWhiteSpace(payload))
                    throw new InvalidOperationException(
                        $"Archive entry {entry.Id} has no payload."
                    );

                StationMessage? message;

                try
                {
                    message = JsonSerializer.Deserialize<StationMessage>(payload, JsonOptions);
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException(
                        $"Cannot deserialize Archive entry {entry.Id}.",
                        ex
                    );
                }

                if (message is null || message.Id <= 0)
                    throw new InvalidOperationException($"Archive entry {entry.Id} is invalid.");

                observedStatuses.Add(await MergeRedisSendStatusAsync(message));
                messages.Add(message);
            }

            _databaseService.SaveMqttMessages(messages);
            _databaseService.SaveFtpMessages(messages);
            _databaseService.SaveFtp2Messages(messages);

            RedisValue[] deleteArgs = new RedisValue[entries.Length * 2];

            for (int i = 0; i < entries.Length; i++)
            {
                deleteArgs[i * 2] = entries[i].Id;
                deleteArgs[i * 2 + 1] = messages[i].Id;
            }

            await _redis.ScriptEvaluateAsync(
                DeleteArchiveEntriesScript,
                new RedisKey[] { ArchiveStream },
                deleteArgs
            );

            for (int i = 0; i < messages.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await DeleteObservedStatusesAsync(messages[i].Id, observedStatuses[i]);
            }

            _logger.LogInformation("Synchronized {Count} Archive messages.", messages.Count);
        }

        await SynchronizePendingSendStatusesAsync(cancellationToken);

        await RestorePendingMqttMessagesAsync(RecoveryBatchSize, cancellationToken);
        await RestorePendingFtpMessagesAsync(RecoveryBatchSize, cancellationToken);
        await RestorePendingFtp2MessagesAsync(RecoveryBatchSize, cancellationToken);
    }

    private async Task SynchronizePendingSendStatusesAsync(CancellationToken cancellationToken)
    {
        Dictionary<long, StationMessage> candidates = new();

        AddCandidates(candidates, _databaseService.GetPendingMqttMessages(ArchiveBatchSize));
        AddCandidates(
            candidates,
            _databaseService.GetPendingFtpMessages(maxMessages: ArchiveBatchSize)
        );
        AddCandidates(
            candidates,
            _databaseService.GetPendingFtp2Messages(maxMessages: ArchiveBatchSize)
        );

        List<StationMessage> updated = new();
        Dictionary<long, bool[]> observedStatuses = new();

        foreach (StationMessage message in candidates.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            bool mqttBefore = message.MqttSent;
            bool ftpBefore = message.FtpSent;
            bool ftp2Before = message.Ftp2Sent;

            observedStatuses[message.Id] = await MergeRedisSendStatusAsync(message);

            if (
                message.MqttSent != mqttBefore
                || message.FtpSent != ftpBefore
                || message.Ftp2Sent != ftp2Before
            )
            {
                updated.Add(message);
            }
        }

        if (updated.Count > 0)
        {
            _databaseService.SaveMqttMessages(updated);
            _databaseService.SaveFtpMessages(updated);
            _databaseService.SaveFtp2Messages(updated);
        }

        foreach (var item in observedStatuses)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (item.Value[0] || item.Value[1] || item.Value[2])
                await DeleteObservedStatusesAsync(item.Key, item.Value);
        }

        await CleanupOrphanRedisSendStatusesAsync(cancellationToken);
    }

    private static void AddCandidates(
        Dictionary<long, StationMessage> candidates,
        IEnumerable<StationMessage> messages
    )
    {
        foreach (StationMessage message in messages)
        {
            if (message.Id > 0)
                candidates.TryAdd(message.Id, message);
        }
    }

    private async Task<bool[]> MergeRedisSendStatusAsync(StationMessage message)
    {
        RedisValue[] values = await _redis.StringGetAsync(
            new RedisKey[]
            {
                $"{MqttStatusPrefix}{message.Id}",
                $"{FtpStatusPrefix}{message.Id}",
                $"{Ftp2StatusPrefix}{message.Id}",
            }
        );

        bool[] observed =
        [
            values[0].ToString() == "1",
            values[1].ToString() == "1",
            values[2].ToString() == "1",
        ];

        if (observed[0])
            message.MqttSent = true;
        if (observed[1])
            message.FtpSent = true;
        if (observed[2])
            message.Ftp2Sent = true;

        return observed;
    }

    private async Task DeleteObservedStatusesAsync(long messageId, bool[] observed)
    {
        if (observed.Length != 3)
            throw new ArgumentException(
                "Exactly three status flags are required.",
                nameof(observed)
            );

        if (!observed[0] && !observed[1] && !observed[2])
            return;

        await _redis.ScriptEvaluateAsync(
            DeleteObservedSendStatusesScript,
            new RedisKey[]
            {
                $"{MqttStatusPrefix}{messageId}",
                $"{FtpStatusPrefix}{messageId}",
                $"{Ftp2StatusPrefix}{messageId}",
            },
            new RedisValue[] { observed[0] ? 1 : 0, observed[1] ? 1 : 0, observed[2] ? 1 : 0 }
        );
    }

    private async Task CleanupOrphanRedisSendStatusesAsync(CancellationToken cancellationToken)
    {
        HashSet<long> messageIds = new();
        string[] prefixes = [MqttStatusPrefix, FtpStatusPrefix, Ftp2StatusPrefix];

        foreach (EndPoint endpoint in _multiplexer.GetEndPoints())
        {
            cancellationToken.ThrowIfCancellationRequested();

            IServer server = _multiplexer.GetServer(endpoint);
            if (!server.IsConnected)
                continue;

            foreach (string prefix in prefixes)
            {
                foreach (
                    RedisKey key in server.Keys(
                        database: _redis.Database,
                        pattern: prefix + "*",
                        pageSize: 500
                    )
                )
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    string keyName = key.ToString();

                    if (long.TryParse(keyName[prefix.Length..], out long id) && id > 0)
                    {
                        messageIds.Add(id);
                    }
                }
            }
        }

        int deleted = 0;

        foreach (long id in messageIds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_databaseService.MessageExists(id))
                continue;

            await DeleteObservedStatusesAsync(id, [true, true, true]);
            deleted++;
        }

        if (deleted > 0)
            _logger.LogInformation("Removed status keys for {Count} missing messages.", deleted);
    }

    public Task<int> RestorePendingMqttMessagesAsync(
        int maxMessages = RecoveryBatchSize,
        CancellationToken cancellationToken = default
    ) => RestorePendingMessagesWhenEmptyAsync(MqttStream, "mqtt", maxMessages, cancellationToken);

    public Task<int> RestorePendingFtpMessagesAsync(
        int maxMessages = RecoveryBatchSize,
        CancellationToken cancellationToken = default
    ) => RestorePendingMessagesWhenEmptyAsync(FtpStream, "ftp", maxMessages, cancellationToken);

    public Task<int> RestorePendingFtp2MessagesAsync(
        int maxMessages = RecoveryBatchSize,
        CancellationToken cancellationToken = default
    ) => RestorePendingMessagesWhenEmptyAsync(Ftp2Stream, "ftp2", maxMessages, cancellationToken);

    private async Task<int> RestorePendingMessagesWhenEmptyAsync(
        string streamName,
        string destination,
        int maxMessages,
        CancellationToken cancellationToken
    )
    {
        if (maxMessages <= 0)
            return 0;

        cancellationToken.ThrowIfCancellationRequested();

        long streamLength = await _redis.StreamLengthAsync(streamName);

        if (streamLength != 0)
        {
            _logger.LogDebug(
                "Skip {Destination} recovery: stream has {Count} entries.",
                destination,
                streamLength
            );
            return 0;
        }

        int batchSize = Math.Min(maxMessages, RecoveryBatchSize);

        List<StationMessage> pending = destination switch
        {
            "mqtt" => _databaseService.GetPendingMqttMessages(batchSize),
            "ftp" => _databaseService.GetPendingFtpMessages(maxMessages: batchSize),
            "ftp2" => _databaseService.GetPendingFtp2Messages(maxMessages: batchSize),
            _ => throw new ArgumentException(
                $"Unsupported destination: {destination}",
                nameof(destination)
            ),
        };

        int restored = 0;

        foreach (StationMessage message in pending)
        {
            cancellationToken.ThrowIfCancellationRequested();

            bool alreadySent = destination switch
            {
                "mqtt" => message.MqttSent,
                "ftp" => message.FtpSent,
                "ftp2" => message.Ftp2Sent,
                _ => true,
            };

            if (
                !alreadySent
                && await EnqueuePendingToDestinationAsync(message, streamName, destination)
            )
            {
                restored++;
            }
        }

        // _logger.LogInformation(
        //     "Recovery {Destination}: loaded {Loaded}, restored {Restored}.",
        //     destination,
        //     pending.Count,
        //     restored
        // );

        return restored;
    }

    private async Task<bool> EnqueuePendingToDestinationAsync(
        StationMessage message,
        string streamName,
        string destination
    )
    {
        ValidateMessage(message);

        if (message.Id <= 0)
            throw new ArgumentException("Message Id must be greater than zero.", nameof(message));

        RedisResult result = await _redis.ScriptEvaluateAsync(
            EnqueuePendingScript,
            new RedisKey[] { streamName },
            new RedisValue[]
            {
                message.Id,
                JsonSerializer.Serialize(message, JsonOptions),
                MaxQueueSize,
                destination,
            }
        );

        return (long)result == 1;
    }

    public async Task<IReadOnlyList<StationMessage>> ReadStreamMessagesAsync(
        string streamName,
        int maxMessages = 100
    )
    {
        ValidateDeliveryStream(streamName);

        if (maxMessages <= 0)
            return Array.Empty<StationMessage>();

        StreamEntry[] entries = await _redis.StreamRangeAsync(
            streamName,
            minId: "-",
            maxId: "+",
            count: maxMessages,
            messageOrder: Order.Ascending
        );

        List<StationMessage> messages = new(entries.Length);

        foreach (StreamEntry entry in entries)
        {
            string? payload = GetField(entry, "payload");
            if (string.IsNullOrWhiteSpace(payload))
                continue;

            try
            {
                StationMessage? message = JsonSerializer.Deserialize<StationMessage>(
                    payload,
                    JsonOptions
                );

                if (message is not null)
                    messages.Add(message);
            }
            catch (JsonException ex)
            {
                _logger.LogError(
                    ex,
                    "Cannot deserialize {StreamName} entry {EntryId}.",
                    streamName,
                    entry.Id
                );
            }
        }

        return messages;
    }

    private static void ValidateDeliveryStream(string streamName)
    {
        if (streamName != MqttStream && streamName != FtpStream && streamName != Ftp2Stream)
            throw new ArgumentException("Unsupported delivery stream.", nameof(streamName));
    }

    private static string? GetField(StreamEntry entry, string fieldName)
    {
        foreach (NameValueEntry field in entry.Values)
        {
            if (field.Name == fieldName)
                return field.Value.IsNull ? null : field.Value.ToString();
        }

        return null;
    }

    private const string EnqueueNewMessageScript = """
        local archive = KEYS[1]
        local mqtt = KEYS[2]
        local ftp = KEYS[3]
        local ftp2 = KEYS[4]

        local messageId = ARGV[1]
        local payload = ARGV[2]
        local maxSize = tonumber(ARGV[3])

        local archiveMarker = 'database:dedup:archive:' .. messageId

        if redis.call('EXISTS', archiveMarker) == 1 then
            return 0
        end

        local archiveEntryId = redis.call(
            'XADD', archive, '*',
            'message_id', messageId,
            'payload', payload
        )

        redis.call('SET', archiveMarker, archiveEntryId)

        local function trimStream(stream, destination)
            while redis.call('XLEN', stream) > maxSize do
                local oldest = redis.call('XRANGE', stream, '-', '+', 'COUNT', 1)
                if #oldest == 0 then break end

                local oldEntryId = oldest[1][1]
                local oldFields = oldest[1][2]
                local oldMessageId = nil

                for i = 1, #oldFields, 2 do
                    if oldFields[i] == 'message_id' then
                        oldMessageId = oldFields[i + 1]
                        break
                    end
                end

                redis.call('XDEL', stream, oldEntryId)

                if oldMessageId then
                    redis.call(
                        'DEL',
                        'database:dedup:' .. destination .. ':' .. oldMessageId
                    )
                end
            end
        end

        trimStream(archive, 'archive')

        local function enqueue(stream, destination, sent)
            if sent == '1' then return end

            local marker = 'database:dedup:' .. destination .. ':' .. messageId
            local statusKey = 'database:status:' .. destination .. ':' .. messageId

            if redis.call('EXISTS', marker) == 1 then return end
            if redis.call('GET', statusKey) == '1' then return end

            redis.call(
                'XADD', stream, '*',
                'message_id', messageId,
                'payload', payload
            )

            redis.call('SET', marker, '1')
            trimStream(stream, destination)
        end

        enqueue(mqtt, 'mqtt', ARGV[4])
        enqueue(ftp, 'ftp', ARGV[5])
        enqueue(ftp2, 'ftp2', ARGV[6])

        return 1
        """;

    private const string EnqueuePendingScript = """
        local stream = KEYS[1]
        local messageId = ARGV[1]
        local payload = ARGV[2]
        local maxSize = tonumber(ARGV[3])
        local destination = ARGV[4]

        local statusKey = 'database:status:' .. destination .. ':' .. messageId
        if redis.call('GET', statusKey) == '1' then return 0 end

        local marker = 'database:dedup:' .. destination .. ':' .. messageId
        if redis.call('EXISTS', marker) == 1 then return 0 end

        redis.call(
            'XADD', stream, '*',
            'message_id', messageId,
            'payload', payload
        )

        redis.call('SET', marker, '1')

        while redis.call('XLEN', stream) > maxSize do
            local oldest = redis.call('XRANGE', stream, '-', '+', 'COUNT', 1)
            if #oldest == 0 then break end

            local oldEntryId = oldest[1][1]
            local oldFields = oldest[1][2]
            local oldMessageId = nil

            for i = 1, #oldFields, 2 do
                if oldFields[i] == 'message_id' then
                    oldMessageId = oldFields[i + 1]
                    break
                end
            end

            redis.call('XDEL', stream, oldEntryId)

            if oldMessageId then
                redis.call(
                    'DEL',
                    'database:dedup:' .. destination .. ':' .. oldMessageId
                )
            end
        end

        return 1
        """;

    private const string DeleteArchiveEntriesScript = """
        for i = 1, #ARGV, 2 do
            redis.call('XDEL', KEYS[1], ARGV[i])
            redis.call('DEL', 'database:dedup:archive:' .. ARGV[i + 1])
        end

        return #ARGV / 2
        """;

    private const string DeleteObservedSendStatusesScript = """
        local deleted = 0

        for i = 1, 3 do
            if ARGV[i] == '1' and redis.call('GET', KEYS[i]) == '1' then
                redis.call('DEL', KEYS[i])
                deleted = deleted + 1
            end
        end

        return deleted
        """;
}
