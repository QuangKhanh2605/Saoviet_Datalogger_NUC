using System.Text.Json;
using Microsoft.Extensions.Options;
using Mqtt.Configuration;
using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Models;
using StackExchange.Redis;

namespace Mqtt.Mqtt.Publishing;

public sealed class MqttPublishManager
{
    // ============================================================
    // CẤU HÌNH
    // ============================================================

    private const int PublishDelayMilliseconds = 2000;
    private const int MaxPublishFailures = 5;
    private const int DatabaseBatchSize = 10;
    private const string MqttStream = "database:stream:mqtt";

    // ============================================================
    // MQTT PUBLISH PRIORITY
    // ============================================================

    private static readonly string[] PublishPriority =
    {
        MqttTopics.MePDV,
        MqttTopics.Opera,
        MqttTopics.sCoFi,
        MqttTopics.sSet1,
    };

    // ============================================================
    // FIELDS
    // ============================================================

    private readonly ILogger<MqttPublishManager> _logger;
    private readonly MqttPublisher _publisher;
    private readonly MqttConnectionManager _connection;
    private readonly IDatabase _redis;
    private readonly AppSettings _settings;
    private readonly SemaphoreSlim _publishSignal = new(0, 1);
    private readonly object _lock = new();
    private readonly Dictionary<string, PendingPublish> _pending = new();
    private readonly Dictionary<string, PublishDefinition> _definitions;

    // ============================================================
    // CONSTRUCTOR
    // ============================================================

    public MqttPublishManager(
        ILogger<MqttPublishManager> logger,
        MqttPublisher publisher,
        MqttConnectionManager connection,
        IConnectionMultiplexer redis,
        IOptions<AppSettings> options
    )
    {
        _logger = logger;
        _publisher = publisher;
        _connection = connection;
        _redis = redis.GetDatabase();
        _settings = options.Value;

        _definitions = new Dictionary<string, PublishDefinition>
        {
            {
                MqttTopics.MePDV,
                new PublishDefinition
                {
                    Prepare = PrepareMePDV,
                    OnSuccessAsync = OnMePDVSuccessAsync,
                }
            },
            {
                MqttTopics.Opera,
                new PublishDefinition
                {
                    PrepareAsync = PrepareOperaAsync,
                    OnSuccessAsync = OnOperaSuccessAsync,
                }
            },
            {
                MqttTopics.sCoFi,
                new PublishDefinition
                {
                    Prepare = PrepareSCoFi,
                    OnSuccessAsync = OnSCoFiSuccessAsync,
                }
            },
            {
                MqttTopics.sSet1,
                new PublishDefinition { OnSuccessAsync = OnSSet1SuccessAsync }
            },
        };
    }

    // ============================================================
    // REQUEST
    // ============================================================

    public void Request(string topic, string? payload = null)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("MQTT topic cannot be empty.", nameof(topic));
        }

        lock (_lock)
        {
            _pending[topic] = new PendingPublish
            {
                Topic = topic,
                Payload = payload,
                Data = null,
                ConsecutiveFailures = 0,
                Published = false,
            };

            if (_publishSignal.CurrentCount == 0)
            {
                _publishSignal.Release();
            }
        }
    }

    // ============================================================
    // MAIN LOOP
    // ============================================================

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // Chờ MQTT READY
            if (!_connection.IsReady)
            {
                await Task.Delay(500, cancellationToken);
                continue;
            }

            // Kiểm tra Redis định kỳ
            await _publishSignal.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken);

            await PrepareDatabaseTopicsAsync(cancellationToken);

            // Xử lý pending MQTT publish
            await ProcessPendingAsync(cancellationToken);
        }
    }

    // ============================================================
    // PROCESS PENDING
    // ============================================================

    private async Task ProcessPendingAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            PendingPublish? pending = GetNextPending();

            if (pending == null)
            {
                return;
            }

            if (!_definitions.TryGetValue(pending.Topic, out PublishDefinition? definition))
            {
                _logger.LogWarning(
                    "MQTT topic has no publish definition | Topic={Topic}",
                    pending.Topic
                );

                RemovePending(pending.Topic);
                continue;
            }

            // ----------------------------------------------------
            // Chuẩn bị payload
            // ----------------------------------------------------

            if (pending.Payload == null)
            {
                if (definition.PrepareAsync != null)
                {
                    await definition.PrepareAsync(pending, cancellationToken);
                }
                else if (definition.Prepare != null)
                {
                    definition.Prepare(pending);
                }
            }

            // ----------------------------------------------------
            // Không có dữ liệu để gửi
            // ----------------------------------------------------

            if (pending.Payload == null)
            {
                RemovePending(pending.Topic);
                continue;
            }

            // ----------------------------------------------------
            // PUBLISH
            // ----------------------------------------------------
            //
            // Chỉ publish một lần cho mỗi PendingPublish.
            //
            // Nếu MQTT đã publish thành công nhưng callback Redis
            // thất bại, vòng lặp tiếp theo chỉ retry callback,
            // không gửi lại payload MQTT.
            // ----------------------------------------------------

            if (!pending.Published)
            {
                MqttPublishRequest request = new()
                {
                    Topic = pending.Topic,
                    Payload = pending.Payload,
                };

                bool success = await _publisher.PublishAsync(request, cancellationToken);

                // Publish thất bại
                if (!success)
                {
                    await HandlePublishFailureAsync(pending, cancellationToken);

                    return;
                }

                // Publish thành công: ghi nhớ trạng thái trong RAM.
                pending.Published = true;
                pending.ConsecutiveFailures = 0;
            }

            // ----------------------------------------------------
            // CALLBACK XÁC NHẬN
            // ----------------------------------------------------

            bool callbackSuccess = true;

            if (definition.OnSuccessAsync != null)
            {
                callbackSuccess = await definition.OnSuccessAsync(pending, cancellationToken);
            }

            // ----------------------------------------------------
            // Callback thất bại
            // ----------------------------------------------------

            if (!callbackSuccess)
            {
                _logger.LogWarning(
                    "MQTT {Topic} published, but success callback failed. "
                        + "Keeping pending batch without republishing.",
                    pending.Topic
                );

                await Task.Delay(PublishDelayMilliseconds, cancellationToken);

                return;
            }

            _logger.LogInformation(
                "MQTT {Topic} published and acknowledged successfully.",
                pending.Topic
            );

            RemovePending(pending.Topic);

            // Lấy batch tiếp theo từ Redis
            await PrepareDatabaseTopicsAsync(cancellationToken);

            // Delay trước batch tiếp theo
            if (HasPending())
            {
                await Task.Delay(PublishDelayMilliseconds, cancellationToken);
            }
        }
    }

    // ============================================================
    // HANDLE PUBLISH FAILURE
    // ============================================================

    private async Task HandlePublishFailureAsync(
        PendingPublish pending,
        CancellationToken cancellationToken
    )
    {
        pending.ConsecutiveFailures++;

        _logger.LogWarning(
            "MQTT publish failed | Topic={Topic} | Failure={Failure}/{Max}",
            pending.Topic,
            pending.ConsecutiveFailures,
            MaxPublishFailures
        );

        // Chưa đủ 5 lần: giữ nguyên payload để retry.
        if (pending.ConsecutiveFailures < MaxPublishFailures)
        {
            await Task.Delay(PublishDelayMilliseconds, cancellationToken);

            return;
        }

        // Đủ 5 lần: yêu cầu reconnect.
        // Không xóa pending.
        pending.ConsecutiveFailures = 0;

        _logger.LogWarning(
            "MQTT publish failed {Max} consecutive times. "
                + "Requesting reconnect | Topic={Topic}",
            MaxPublishFailures,
            pending.Topic
        );

        _connection.RequestReconnect(switchServer: true);
    }

    // ============================================================
    // PREPARE DATABASE TOPICS
    // ============================================================

    private async Task PrepareDatabaseTopicsAsync(CancellationToken cancellationToken)
    {
        foreach (string topic in PublishPriority)
        {
            if (!_definitions.TryGetValue(topic, out PublishDefinition? definition))
            {
                continue;
            }

            if (definition.PrepareAsync == null)
            {
                continue;
            }

            if (HasPending(topic))
            {
                continue;
            }

            PendingPublish pending = new()
            {
                Topic = topic,
                Payload = null,
                Data = null,
                ConsecutiveFailures = 0,
                Published = false,
            };

            await definition.PrepareAsync(pending, cancellationToken);

            if (pending.Payload == null)
            {
                continue;
            }

            lock (_lock)
            {
                if (!_pending.ContainsKey(topic))
                {
                    _pending[topic] = pending;
                }
            }

            if (_publishSignal.CurrentCount == 0)
            {
                _publishSignal.Release();
            }
        }
    }

    // ============================================================
    // PREPARE MePDV
    // ============================================================

    private void PrepareMePDV(PendingPublish pending)
    {
        var data = new
        {
            // SaveIntervalMinutes = _settings.Database.SaveIntervalMinutes,
            Ver = _settings.Device.Ver,
            Server = _connection.CurrentServerIp,
        };

        pending.Payload = JsonSerializer.Serialize(data);

        _logger.LogDebug("MQTT MePDV prepared | Server={Server}", _connection.CurrentServerIp);
    }

    // ============================================================
    // PREPARE OPERA - READ FROM REDIS STREAM
    // ============================================================

    private async Task PrepareOperaAsync(
        PendingPublish pending,
        CancellationToken cancellationToken
    )
    {
        if (pending.Payload != null)
        {
            return;
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Đọc tối đa 10 entry từ Redis Stream.
            // Mỗi entry phải có field "payload" chứa JSON StationMessageDto.

            StreamEntry[] entries = await _redis.StreamRangeAsync(
                MqttStream,
                minId: "-",
                maxId: "+",
                count: DatabaseBatchSize,
                messageOrder: Order.Ascending
            );

            if (entries.Length == 0)
            {
                _logger.LogDebug("MQTT Opera Redis check | No pending messages.");

                return;
            }

            List<StationMessageDto> messages = new(entries.Length);
            List<RedisMessageReference> references = new(entries.Length);

            foreach (StreamEntry entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Tìm field payload trong Redis Stream entry.
                RedisValue payloadValue = RedisValue.Null;

                foreach (NameValueEntry field in entry.Values)
                {
                    if (field.Name == "payload")
                    {
                        payloadValue = field.Value;
                        break;
                    }
                }

                if (payloadValue.IsNullOrEmpty)
                {
                    _logger.LogWarning(
                        "MQTT Opera Redis entry has no payload | EntryId={EntryId}",
                        entry.Id
                    );

                    continue;
                }

                StationMessageDto? message;

                try
                {
                    message = JsonSerializer.Deserialize<StationMessageDto>(
                        payloadValue.ToString()
                    );
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "MQTT Opera Redis payload is invalid | EntryId={EntryId}",
                        entry.Id
                    );

                    continue;
                }

                if (message == null || message.Id <= 0)
                {
                    _logger.LogWarning(
                        "MQTT Opera Redis message is invalid | EntryId={EntryId}",
                        entry.Id
                    );

                    continue;
                }

                messages.Add(message);

                references.Add(
                    new RedisMessageReference
                    {
                        EntryId = entry.Id.ToString(),
                        MessageId = message.Id,
                    }
                );
            }

            if (messages.Count == 0)
            {
                return;
            }

            // Giữ nguyên cấu trúc JSON Opera cũ.
            // Không đưa MessageId vào payload MQTT.

            var data = messages
                .Select(message => new
                {
                    StationName = message.StationName,
                    Timestamp = message.Timestamp,

                    Measurements = message
                        .Measurements.Select(measurement => new
                        {
                            measurement.Name,
                            measurement.Value,
                            measurement.Unit,
                            measurement.Status,
                        })
                        .ToList(),
                })
                .ToList();

            pending.Payload = JsonSerializer.Serialize(data);

            // Lưu tham chiếu Redis để xóa entry sau khi publish thành công.
            pending.Data = references;

            _logger.LogDebug(
                "MQTT Opera prepared from Redis Stream | "
                    + "BatchSize={BatchSize} | Stations={Stations} | EntryIds={EntryIds}",
                messages.Count,
                string.Join(", ", messages.Select(x => x.StationName)),
                string.Join(", ", references.Select(x => x.EntryId))
            );
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MQTT Opera Redis read failed.");
        }
    }

    // ============================================================
    // PREPARE sCoFi
    // ============================================================

    private void PrepareSCoFi(PendingPublish pending) { }

    // ============================================================
    // SUCCESS CALLBACK - MePDV
    // ============================================================

    private Task<bool> OnMePDVSuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken
    )
    {
        return Task.FromResult(true);
    }

    // ============================================================
    // SUCCESS CALLBACK - OPERA
    // ACKNOWLEDGE AND DELETE FROM REDIS
    // ============================================================

    private const string MarkOperaSentScript = """
        local stream = KEYS[1]

        local statusPrefix = 'database:status:mqtt:'
        local dedupPrefix = 'database:dedup:mqtt:'
        local processed = 0

        for i = 1, #ARGV, 2 do
            local entryId = ARGV[i]
            local messageId = ARGV[i + 1]

            -- XDEL trả về 0 nếu entry không còn tồn tại.
            -- Trường hợp này vẫn được xem là đã xử lý.
            --
            -- DEL cũng hợp lệ khi dedup key không tồn tại.
            -- Đặt status sau khi hai lệnh trên chạy thành công.

            redis.call('XDEL', stream, entryId)
            redis.call('DEL', dedupPrefix .. messageId)
            redis.call('SET', statusPrefix .. messageId, '1')

            -- Chỉ cần các lệnh Redis thực thi không lỗi thì
            -- được tính là xử lý thành công, kể cả khi entry
            -- hoặc dedup key đã không còn tồn tại.

            processed = processed + 1
        end

        return processed
        """;

    private async Task<bool> OnOperaSuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken
    )
    {
        if (pending.Data is not List<RedisMessageReference> references || references.Count == 0)
        {
            return true;
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Truyền từng cặp EntryId và MessageId vào Lua.
            // Lua thực hiện xóa Stream, xóa dedup và tạo status
            // trong một thao tác nguyên tử trên Redis.

            RedisValue[] args = references
                .SelectMany(reference =>
                    new RedisValue[] { reference.EntryId, reference.MessageId.ToString() }
                )
                .ToArray();

            RedisResult result = await _redis.ScriptEvaluateAsync(
                MarkOperaSentScript,
                new RedisKey[] { MqttStream },
                args
            );

            long processed = (long)result;

            _logger.LogInformation(
                "MQTT Opera Redis batch marked as sent | "
                    + "Count={Count} | Processed={Processed} | MessageIds={MessageIds}",
                references.Count,
                processed,
                string.Join(", ", references.Select(x => x.MessageId))
            );

            return processed == references.Count;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "MQTT Opera Redis acknowledgement failed | MessageIds={MessageIds}",
                string.Join(", ", references.Select(x => x.MessageId))
            );

            return false;
        }
    }

    // ============================================================
    // SUCCESS CALLBACK - sCoFi
    // ============================================================

    private Task<bool> OnSCoFiSuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken
    )
    {
        return Task.FromResult(true);
    }

    // ============================================================
    // SUCCESS CALLBACK - sSet1
    // ============================================================

    private Task<bool> OnSSet1SuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken
    )
    {
        return Task.FromResult(true);
    }

    // ============================================================
    // GET NEXT PENDING
    // ============================================================

    private PendingPublish? GetNextPending()
    {
        lock (_lock)
        {
            foreach (string topic in PublishPriority)
            {
                if (_pending.TryGetValue(topic, out PendingPublish? pending))
                {
                    return pending;
                }
            }

            return null;
        }
    }

    // ============================================================
    // HAS PENDING
    // ============================================================

    private bool HasPending()
    {
        lock (_lock)
        {
            return _pending.Count > 0;
        }
    }

    private bool HasPending(string topic)
    {
        lock (_lock)
        {
            return _pending.ContainsKey(topic);
        }
    }

    // ============================================================
    // REMOVE PENDING
    // ============================================================

    private void RemovePending(string topic)
    {
        lock (_lock)
        {
            _pending.Remove(topic);
        }
    }

    // ============================================================
    // INNER TYPES
    // ============================================================

    private sealed class PendingPublish
    {
        public string Topic { get; set; } = string.Empty;

        public string? Payload { get; set; }

        // Opera: danh sách tham chiếu tới entry Redis đã đọc.
        public object? Data { get; set; }

        public int ConsecutiveFailures { get; set; }

        // MQTT publish đã thành công.
        // Retry callback không được publish lại payload.
        public bool Published { get; set; }
    }

    private sealed class PublishDefinition
    {
        public Action<PendingPublish>? Prepare { get; init; }

        public Func<PendingPublish, CancellationToken, Task>? PrepareAsync { get; init; }

        public Func<PendingPublish, CancellationToken, Task<bool>>? OnSuccessAsync { get; init; }
    }

    // ============================================================
    // REDIS MESSAGE REFERENCE
    // ============================================================

    private sealed class RedisMessageReference
    {
        public string EntryId { get; set; } = string.Empty;

        public long MessageId { get; set; }
    }

    // ============================================================
    // DATABASE MESSAGE DTO
    // ============================================================

    private sealed class StationMessageDto
    {
        public long Id { get; set; }

        public string Timestamp { get; set; } = "";

        public string StationName { get; set; } = "";

        public bool MqttSent { get; set; }

        public bool FtpSent { get; set; }

        public bool Ftp2Sent { get; set; }

        public List<SensorReadingDto> Measurements { get; set; } = new();
    }

    private sealed class SensorReadingDto
    {
        public string StationName { get; set; } = "";

        public byte SlaveId { get; set; }

        public string Name { get; set; } = "";

        public double Value { get; set; }

        public string Unit { get; set; } = "";

        public int Status { get; set; }

        public string Timestamp { get; set; } = "";
    }
}
