using Database.Models;

namespace Database.Services;

public sealed class MessageQueueService
{
    private const int MaxQueueSize = 100;
    private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(5);

    private readonly DatabaseService _databaseService;
    private readonly ILogger<MessageQueueService> _logger;

    // =============================================================
    // 3 QUEUE HOÀN TOÀN ĐỘC LẬP
    // =============================================================

    private readonly LinkedList<QueueItem> _mqttQueue = new();
    private readonly LinkedList<QueueItem> _ftpQueue = new();
    private readonly LinkedList<QueueItem> _ftp2Queue = new();

    // =============================================================
    // 3 LOCK ĐỘC LẬP
    // =============================================================

    private readonly SemaphoreSlim _mqttLock = new(1, 1);
    private readonly SemaphoreSlim _ftpLock = new(1, 1);
    private readonly SemaphoreSlim _ftp2Lock = new(1, 1);

    // =============================================================
    // MESSAGE ID
    // =============================================================

    private long _nextMessageId;

    // =============================================================
    // CONSTRUCTOR
    // =============================================================

    public MessageQueueService(DatabaseService databaseService, ILogger<MessageQueueService> logger)
    {
        _databaseService = databaseService;
        _logger = logger;

        long databaseMaxId = 0;

        try
        {
            databaseMaxId = _databaseService.GetMaxMessageId();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Could not read maximum message ID from SQLite. Using RAM-generated ID seed."
            );
        }

        // Timestamp-based seed để tránh đụng ID cũ
        // khi SQLite chưa đọc được.
        long timeSeed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        _nextMessageId = Math.Max(databaseMaxId, timeSeed);

        _logger.LogInformation(
            "MessageQueueService initialized. NextMessageId={NextMessageId}.",
            _nextMessageId + 1
        );
    }

    // =============================================================
    // QUEUE ITEM
    // =============================================================

    private sealed class QueueItem
    {
        public StationMessage Message { get; }

        // Queue tương ứng đã gửi thành công.
        public bool Sent { get; set; }

        // Message đã được lưu vào SQLite.
        public bool StoredInDatabase { get; set; }

        public QueueItem(StationMessage message, bool sent, bool storedInDatabase)
        {
            Message = message;
            Sent = sent;
            StoredInDatabase = storedInDatabase;
        }
    }

    // =============================================================
    // ENQUEUE NEW STATION MESSAGES
    // =============================================================

    public int EnqueueStationMessages(IEnumerable<StationMessage> messages)
    {
        List<StationMessage> messageList = messages.ToList();

        if (messageList.Count == 0)
        {
            return 0;
        }

        int addedCount = 0;

        foreach (StationMessage message in messageList)
        {
            ValidateMessage(message);

            // =====================================================
            // GENERATE ID BEFORE SQLITE
            // =====================================================

            if (message.Id <= 0)
            {
                message.Id = Interlocked.Increment(ref _nextMessageId);
            }

            // =====================================================
            // MQTT
            // =====================================================

            AcquireLock(_mqttLock);

            try
            {
                AddToQueue(
                    _mqttQueue,
                    CloneMessage(message),
                    message.MqttSent,
                    storedInDatabase: false
                );
            }
            finally
            {
                _mqttLock.Release();
            }

            // =====================================================
            // FTP
            // =====================================================

            AcquireLock(_ftpLock);

            try
            {
                AddToQueue(
                    _ftpQueue,
                    CloneMessage(message),
                    message.FtpSent,
                    storedInDatabase: false
                );
            }
            finally
            {
                _ftpLock.Release();
            }

            // =====================================================
            // FTP2
            // =====================================================

            AcquireLock(_ftp2Lock);

            try
            {
                AddToQueue(
                    _ftp2Queue,
                    CloneMessage(message),
                    message.Ftp2Sent,
                    storedInDatabase: false
                );
            }
            finally
            {
                _ftp2Lock.Release();
            }

            addedCount++;
        }

        return addedCount;
    }

    // =============================================================
    // VALIDATE MESSAGE
    // =============================================================

    private static void ValidateMessage(StationMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.StationName))
        {
            throw new ArgumentException("StationName cannot be empty.");
        }

        if (string.IsNullOrWhiteSpace(message.Timestamp))
        {
            throw new ArgumentException("Timestamp cannot be empty.");
        }
    }

    // =============================================================
    // ADD TO QUEUE
    // =============================================================

    private static void AddToQueue(
        LinkedList<QueueItem> queue,
        StationMessage message,
        bool sent,
        bool storedInDatabase
    )
    {
        // Không duplicate ID trong cùng queue.
        if (FindById(queue, message.Id) != null)
        {
            return;
        }

        queue.AddLast(new QueueItem(message, sent, storedInDatabase));

        EnforceQueueLimit(queue);
    }

    // =============================================================
    // MQTT GET PENDING
    // =============================================================

    public List<StationMessage> GetPendingMqttMessages(int maxMessages)
    {
        if (maxMessages <= 0)
        {
            return [];
        }

        maxMessages = Math.Min(maxMessages, MaxQueueSize);

        AcquireLock(_mqttLock);

        try
        {
            return GetPendingFromQueue(_mqttQueue, maxMessages);
        }
        finally
        {
            _mqttLock.Release();
        }
    }

    // =============================================================
    // FTP GET PENDING
    // =============================================================

    public List<StationMessage> GetPendingFtpMessages(
        IEnumerable<string>? stationNames,
        int maxMessages
    )
    {
        if (maxMessages <= 0)
        {
            return [];
        }

        maxMessages = Math.Min(maxMessages, MaxQueueSize);

        List<string>? stations = NormalizeStationNames(stationNames);

        AcquireLock(_ftpLock);

        try
        {
            return GetPendingFromQueue(_ftpQueue, maxMessages, stations);
        }
        finally
        {
            _ftpLock.Release();
        }
    }

    // =============================================================
    // FTP2 GET PENDING
    // =============================================================

    public List<StationMessage> GetPendingFtp2Messages(
        IEnumerable<string>? stationNames,
        int maxMessages
    )
    {
        if (maxMessages <= 0)
        {
            return [];
        }

        maxMessages = Math.Min(maxMessages, MaxQueueSize);

        List<string>? stations = NormalizeStationNames(stationNames);

        AcquireLock(_ftp2Lock);

        try
        {
            return GetPendingFromQueue(_ftp2Queue, maxMessages, stations);
        }
        finally
        {
            _ftp2Lock.Release();
        }
    }

    // =============================================================
    // GET PENDING INTERNAL
    // =============================================================

    private static List<StationMessage> GetPendingFromQueue(
        LinkedList<QueueItem> queue,
        int maxMessages,
        List<string>? stationNames = null
    )
    {
        var result = new List<StationMessage>(Math.Min(maxMessages, queue.Count));

        foreach (QueueItem item in queue)
        {
            if (item.Sent)
            {
                continue;
            }

            if (
                stationNames is { Count: > 0 }
                && !stationNames.Contains(
                    item.Message.StationName,
                    StringComparer.OrdinalIgnoreCase
                )
            )
            {
                continue;
            }

            result.Add(CloneMessage(item.Message));

            if (result.Count >= maxMessages)
            {
                break;
            }
        }

        return result;
    }

    // =============================================================
    // MARK MQTT SENT
    // =============================================================

    public int MarkMqttSent(IEnumerable<long> messageIds)
    {
        return MarkSent(_mqttQueue, _mqttLock, messageIds, QueueType.Mqtt);
    }

    // =============================================================
    // MARK FTP SENT
    // =============================================================

    public int MarkFtpSent(IEnumerable<long> messageIds)
    {
        return MarkSent(_ftpQueue, _ftpLock, messageIds, QueueType.Ftp);
    }

    // =============================================================
    // MARK FTP2 SENT
    // =============================================================

    public int MarkFtp2Sent(IEnumerable<long> messageIds)
    {
        return MarkSent(_ftp2Queue, _ftp2Lock, messageIds, QueueType.Ftp2);
    }

    // =============================================================
    // MARK SENT INTERNAL
    // =============================================================

    private static int MarkSent(
        LinkedList<QueueItem> queue,
        SemaphoreSlim queueLock,
        IEnumerable<long> messageIds,
        QueueType queueType
    )
    {
        List<long> ids = messageIds.Distinct().ToList();

        if (ids.Count == 0)
        {
            return 0;
        }

        AcquireLock(queueLock);

        try
        {
            int markedCount = 0;

            foreach (long id in ids)
            {
                QueueItem? item = FindById(queue, id);

                if (item == null || item.Sent)
                {
                    continue;
                }

                item.Sent = true;

                SetQueueSentState(item.Message, queueType, true);

                markedCount++;
            }

            return markedCount;
        }
        finally
        {
            queueLock.Release();
        }
    }

    // =============================================================
    // SYNCHRONIZE
    // =============================================================

    public async Task SynchronizeAsync(CancellationToken cancellationToken)
    {
        await SynchronizeMqttAsync(cancellationToken);
        await SynchronizeFtpAsync(cancellationToken);
        await SynchronizeFtp2Async(cancellationToken);
    }

    // =============================================================
    // MQTT SYNCHRONIZATION
    // =============================================================

    private async Task SynchronizeMqttAsync(CancellationToken cancellationToken)
    {
        await SynchronizeQueueAsync(_mqttQueue, _mqttLock, QueueType.Mqtt, cancellationToken);
    }

    // =============================================================
    // FTP SYNCHRONIZATION
    // =============================================================

    private async Task SynchronizeFtpAsync(CancellationToken cancellationToken)
    {
        await SynchronizeQueueAsync(_ftpQueue, _ftpLock, QueueType.Ftp, cancellationToken);
    }

    // =============================================================
    // FTP2 SYNCHRONIZATION
    // =============================================================

    private async Task SynchronizeFtp2Async(CancellationToken cancellationToken)
    {
        await SynchronizeQueueAsync(_ftp2Queue, _ftp2Lock, QueueType.Ftp2, cancellationToken);
    }

    // =============================================================
    // GENERIC QUEUE SYNCHRONIZATION
    // =============================================================

    private async Task SynchronizeQueueAsync(
        LinkedList<QueueItem> queue,
        SemaphoreSlim queueLock,
        QueueType queueType,
        CancellationToken cancellationToken
    )
    {
        bool acquired = await queueLock.WaitAsync(LockTimeout, cancellationToken);

        if (!acquired)
        {
            _logger.LogWarning(
                "{QueueType} queue synchronization skipped because lock was busy.",
                queueType
            );

            return;
        }

        try
        {
            try
            {
                // =================================================
                // RAM → SQLITE
                // =================================================

                SynchronizeQueueToDatabase(queue, queueType);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to synchronize {QueueType} RAM queue to SQLite.",
                    queueType
                );

                return;
            }

            try
            {
                // =================================================
                // SQLITE → RAM
                // =================================================

                SynchronizeDatabaseToQueue(queue, queueType);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to synchronize SQLite to {QueueType} RAM queue.",
                    queueType
                );
            }
        }
        finally
        {
            queueLock.Release();
        }
    }

    // =============================================================
    // RAM → SQLITE
    // =============================================================

    private void SynchronizeQueueToDatabase(LinkedList<QueueItem> queue, QueueType queueType)
    {
        if (queue.Count == 0)
        {
            return;
        }

        List<QueueItem> items = queue.ToList();

        List<StationMessage> messages = items
            .Select(item =>
            {
                StationMessage message = CloneMessage(item.Message);

                SetQueueSentState(message, queueType, item.Sent);

                return message;
            })
            .ToList();

        switch (queueType)
        {
            case QueueType.Mqtt:
                _databaseService.SaveMqttMessages(messages);
                break;

            case QueueType.Ftp:
                _databaseService.SaveFtpMessages(messages);
                break;

            case QueueType.Ftp2:
                _databaseService.SaveFtp2Messages(messages);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(queueType));
        }

        // SQLite save thành công.
        foreach (QueueItem item in items)
        {
            item.StoredInDatabase = true;
        }

        // Chỉ xóa message:
        // 1. Đã lưu SQLite
        // 2. Queue tương ứng đã gửi
        RemoveCompletedItems(queue);
    }

    // =============================================================
    // REMOVE COMPLETED ITEMS
    // =============================================================

    private static void RemoveCompletedItems(LinkedList<QueueItem> queue)
    {
        LinkedListNode<QueueItem>? node = queue.First;

        while (node != null)
        {
            LinkedListNode<QueueItem>? next = node.Next;

            if (node.Value.StoredInDatabase && node.Value.Sent)
            {
                queue.Remove(node);
            }

            node = next;
        }
    }

    // =============================================================
    // SQLITE → RAM
    // =============================================================

    private void SynchronizeDatabaseToQueue(LinkedList<QueueItem> queue, QueueType queueType)
    {
        int available = MaxQueueSize - queue.Count;

        if (available <= 0)
        {
            return;
        }

        List<StationMessage> messages = queueType switch
        {
            QueueType.Mqtt => _databaseService.GetPendingMqttMessages(available),

            QueueType.Ftp => _databaseService.GetPendingFtpMessages(null, available),

            QueueType.Ftp2 => _databaseService.GetPendingFtp2Messages(null, available),

            _ => throw new ArgumentOutOfRangeException(nameof(queueType)),
        };

        foreach (StationMessage message in messages)
        {
            if (FindById(queue, message.Id) != null)
            {
                continue;
            }

            bool sent = GetQueueSentState(message, queueType);

            // Database chỉ trả pending,
            // nhưng vẫn bảo vệ queue.
            if (sent)
            {
                continue;
            }

            queue.AddLast(
                new QueueItem(CloneMessage(message), sent: false, storedInDatabase: true)
            );

            if (queue.Count >= MaxQueueSize)
            {
                break;
            }
        }
    }

    // =============================================================
    // SYNCHRONIZE + MAINTENANCE
    // =============================================================

    public async Task SynchronizeAndMaintainAsync(
        int retentionDays,
        int maxDatabaseSizeMB,
        bool vacuumAfterCleanup,
        CancellationToken cancellationToken
    )
    {
        await SynchronizeAsync(cancellationToken);

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            _databaseService.CleanupDatabase(retentionDays, maxDatabaseSizeMB, vacuumAfterCleanup);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Database cleanup failed.");
        }
    }

    // =============================================================
    // QUEUE LIMIT
    // =============================================================

    private static void EnforceQueueLimit(LinkedList<QueueItem> queue)
    {
        while (queue.Count > MaxQueueSize)
        {
            LinkedListNode<QueueItem>? node = queue.First;

            // Ưu tiên xóa message cũ nhất
            // đã tồn tại trong SQLite.
            while (node != null)
            {
                if (node.Value.StoredInDatabase)
                {
                    break;
                }

                node = node.Next;
            }

            node ??= queue.First;

            if (node == null)
            {
                break;
            }

            queue.Remove(node);
        }
    }

    // =============================================================
    // FIND MESSAGE
    // =============================================================

    private static QueueItem? FindById(LinkedList<QueueItem> queue, long id)
    {
        foreach (QueueItem item in queue)
        {
            if (item.Message.Id == id)
            {
                return item;
            }
        }

        return null;
    }

    // =============================================================
    // STATION FILTER
    // =============================================================

    private static List<string>? NormalizeStationNames(IEnumerable<string>? stationNames)
    {
        List<string>? result = stationNames
            ?.Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        return result is { Count: > 0 } ? result : null;
    }

    // =============================================================
    // LOCK
    // =============================================================

    private static void AcquireLock(SemaphoreSlim queueLock)
    {
        if (!queueLock.Wait(LockTimeout))
        {
            throw new TimeoutException(
                "Database message queue is busy. Lock timeout after 5 seconds."
            );
        }
    }

    // =============================================================
    // QUEUE TYPE
    // =============================================================

    private enum QueueType
    {
        Mqtt,
        Ftp,
        Ftp2,
    }

    // =============================================================
    // GET QUEUE SENT STATE
    // =============================================================

    private static bool GetQueueSentState(StationMessage message, QueueType queueType)
    {
        return queueType switch
        {
            QueueType.Mqtt => message.MqttSent,
            QueueType.Ftp => message.FtpSent,
            QueueType.Ftp2 => message.Ftp2Sent,
            _ => false,
        };
    }

    // =============================================================
    // SET QUEUE SENT STATE
    // =============================================================

    private static void SetQueueSentState(StationMessage message, QueueType queueType, bool sent)
    {
        switch (queueType)
        {
            case QueueType.Mqtt:
                message.MqttSent = sent;
                break;

            case QueueType.Ftp:
                message.FtpSent = sent;
                break;

            case QueueType.Ftp2:
                message.Ftp2Sent = sent;
                break;
        }
    }

    // =============================================================
    // CLONE MESSAGE
    // =============================================================

    private static StationMessage CloneMessage(StationMessage source)
    {
        return new StationMessage
        {
            Id = source.Id,
            Timestamp = source.Timestamp,
            StationName = source.StationName,

            MqttSent = source.MqttSent,
            FtpSent = source.FtpSent,
            Ftp2Sent = source.Ftp2Sent,

            Measurements = source
                .Measurements.Select(reading => new SensorReading
                {
                    StationName = reading.StationName,
                    SlaveId = reading.SlaveId,
                    Name = reading.Name,
                    Value = reading.Value,
                    Unit = reading.Unit,
                    Status = reading.Status,
                    Timestamp = reading.Timestamp,
                })
                .ToList(),
        };
    }
}
