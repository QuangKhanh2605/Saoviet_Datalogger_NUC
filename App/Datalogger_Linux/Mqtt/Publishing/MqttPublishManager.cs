using System.Text.Json;
using Datalogger_Linux.Configuration;
using Datalogger_Linux.Models;
using Datalogger_Linux.Mqtt.Core;
using Datalogger_Linux.Mqtt.Models;
using Datalogger_Linux.Services;
using Microsoft.Extensions.Options;

namespace Datalogger_Linux.Mqtt.Publishing;

public sealed class MqttPublishManager : BackgroundService
{
    // ============================================================
    // CẤU HÌNH
    // ============================================================

    // Khoảng thời gian giữa các lần publish
    private const int PublishDelayMilliseconds = 500;

    // Số lần publish thất bại liên tiếp trước khi yêu cầu reconnect
    private const int MaxPublishFailures = 5;

    // Mỗi lần lấy tối đa 10 StationMessage từ database
    // và đóng gói thành 1 MQTT payload Opera.
    private const int DatabaseBatchSize = 10;

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
    private readonly DatabaseService _database;
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
        DatabaseService database,
        IOptions<AppSettings> options
    )
    {
        _logger = logger;
        _publisher = publisher;
        _connection = connection;
        _database = database;
        _settings = options.Value;

        _definitions = new Dictionary<string, PublishDefinition>
        {
            {
                MqttTopics.MePDV,
                new PublishDefinition
                {
                    Prepare = PrepareMePDV,
                    OnSuccessAsync = OnMePDVSuccessAsync,
                    PrepareFromDatabase = false,
                }
            },
            {
                MqttTopics.Opera,
                new PublishDefinition
                {
                    Prepare = PrepareOpera,
                    OnSuccessAsync = OnOperaSuccessAsync,
                    PrepareFromDatabase = true,
                }
            },
            {
                MqttTopics.sCoFi,
                new PublishDefinition
                {
                    Prepare = PrepareSCoFi,
                    OnSuccessAsync = OnSCoFiSuccessAsync,
                    PrepareFromDatabase = false,
                }
            },
            {
                MqttTopics.sSet1,
                new PublishDefinition
                {
                    Prepare = _ => { },
                    OnSuccessAsync = _ => Task.CompletedTask,
                    PrepareFromDatabase = false,
                }
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
            };

            // Semaphore chỉ cần signal một lần.
            if (_publishSignal.CurrentCount == 0)
            {
                _publishSignal.Release();
            }
        }
    }

    // ============================================================
    // BACKGROUND LOOP
    // ============================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("MQTT PublishManager started.");

        try
        {
            await RunAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Application shutdown bình thường.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MQTT PublishManager stopped because of unexpected error.");
        }

        _logger.LogInformation("MQTT PublishManager stopped.");
    }

    // ============================================================
    // MAIN LOOP
    // ============================================================

    // PUBLIC:
    // MqttWorker hiện tại đang gọi RunAsync().
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // ----------------------------------------------------
            // Chờ MQTT READY
            // ----------------------------------------------------

            if (!_connection.IsReady)
            {
                await Task.Delay(500, cancellationToken);

                continue;
            }

            // ----------------------------------------------------
            // Kiểm tra database định kỳ
            // ----------------------------------------------------

            await _publishSignal.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken);

            PrepareDatabaseTopics();

            // ----------------------------------------------------
            // Xử lý các pending MQTT publish
            // ----------------------------------------------------

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
            // Chuẩn bị payload nếu chưa có
            // ----------------------------------------------------

            if (pending.Payload == null)
            {
                definition.Prepare(pending);
            }

            // Không có dữ liệu để gửi.
            if (pending.Payload == null)
            {
                RemovePending(pending.Topic);

                continue;
            }

            // ----------------------------------------------------
            // Publish
            // ----------------------------------------------------

            MqttPublishRequest request = new() { Topic = pending.Topic, Payload = pending.Payload };

            bool success = await _publisher.PublishAsync(request, cancellationToken);

            // ----------------------------------------------------
            // Publish thất bại
            // ----------------------------------------------------

            if (!success)
            {
                await HandlePublishFailureAsync(pending, cancellationToken);

                return;
            }

            // ----------------------------------------------------
            // Publish thành công
            // ----------------------------------------------------

            pending.ConsecutiveFailures = 0;

            await definition.OnSuccessAsync(pending);

            _logger.LogInformation("MQTT {Topic} published successfully.", pending.Topic);

            RemovePending(pending.Topic);

            // ----------------------------------------------------
            // Sau khi gửi thành công:
            // kiểm tra tiếp database để lấy batch tiếp theo.
            // ----------------------------------------------------

            PrepareDatabaseTopics();

            // ----------------------------------------------------
            // Nếu còn message thì delay trước khi gửi tiếp.
            // ----------------------------------------------------

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
            "MQTT publish failed | " + "Topic={Topic} | " + "Failure={Failure}/{Max}",
            pending.Topic,
            pending.ConsecutiveFailures,
            MaxPublishFailures
        );

        // --------------------------------------------------------
        // Chưa đủ 5 lần thất bại:
        // giữ nguyên payload để retry.
        // --------------------------------------------------------

        if (pending.ConsecutiveFailures < MaxPublishFailures)
        {
            await Task.Delay(PublishDelayMilliseconds, cancellationToken);

            return;
        }

        // --------------------------------------------------------
        // Đã thất bại 5 lần:
        // yêu cầu MQTT ConnectionManager reconnect.
        //
        // Không xóa pending.
        // Payload batch vẫn được giữ lại để gửi lại.
        // --------------------------------------------------------

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

    private void PrepareDatabaseTopics()
    {
        foreach (string topic in PublishPriority)
        {
            if (!_definitions.TryGetValue(topic, out PublishDefinition? definition))
            {
                continue;
            }

            if (!definition.PrepareFromDatabase)
            {
                continue;
            }

            // Đã có pending thì không load thêm.
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
            };

            definition.Prepare(pending);

            // Không có dữ liệu database.
            if (pending.Payload == null)
            {
                continue;
            }

            lock (_lock)
            {
                // Kiểm tra lại vì có thể thread khác vừa tạo pending.
                if (!_pending.ContainsKey(topic))
                {
                    _pending[topic] = pending;
                }
            }

            // Signal để ProcessPendingAsync xử lý ngay.
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
            SaveIntervalMinutes = _settings.Database.SaveIntervalMinutes,

            Ver = _settings.Device.Ver,

            Server = _connection.CurrentServerIp,
        };

        pending.Payload = JsonSerializer.Serialize(data);

        _logger.LogDebug("MQTT MePDV prepared | Server={Server}", _connection.CurrentServerIp);
    }

    // ============================================================
    // PREPARE OPERA
    // ============================================================

    private void PrepareOpera(PendingPublish pending)
    {
        if (pending.Payload != null)
        {
            return;
        }

        // --------------------------------------------------------
        // Lấy tối đa DatabaseBatchSize message từ SQLite.
        //
        // DatabaseBatchSize = 10
        //
        // => lấy tối đa 10 StationMessage.
        // --------------------------------------------------------

        List<StationMessage> messages = _database.GetPendingMqttMessages(DatabaseBatchSize);

        if (messages.Count == 0)
        {
            _logger.LogDebug("MQTT Opera database check | " + "No pending messages.");

            return;
        }

        // --------------------------------------------------------
        // Đóng gói tối đa 10 station thành 1 JSON ARRAY.
        //
        // Mỗi phần tử:
        //
        // {
        //     StationName,
        //     Timestamp,
        //     Measurements
        // }
        //
        // Thứ tự JSON:
        // StationName -> Timestamp -> Measurements
        //
        // Không đưa Id/MqttSent/FtpSent vào payload.
        // --------------------------------------------------------

        var data = messages
            .Select(message => new
            {
                StationName = message.StationName,

                Timestamp = message.Timestamp,

                Measurements = message
                    .Measurements.Select(measurement => new
                    {
                        measurement.ParameterName,
                        measurement.Value,
                        measurement.Unit,
                        measurement.Status,
                    })
                    .ToList(),
            })
            .ToList();

        pending.Payload = JsonSerializer.Serialize(data);

        // --------------------------------------------------------
        // Lưu toàn bộ message_id của batch.
        //
        // Ví dụ:
        //
        // [101, 102, 103, ..., 110]
        //
        // Khi publish thành công:
        // MarkMqttSent() sẽ đánh dấu toàn bộ batch.
        // --------------------------------------------------------

        pending.Data = messages.Select(message => message.Id).ToList();

        _logger.LogDebug(
            "MQTT Opera prepared | " + "BatchSize={BatchSize} | " + "Stations={Stations}",
            messages.Count,
            string.Join(", ", messages.Select(x => x.StationName))
        );
    }

    // ============================================================
    // PREPARE sCoFi
    // ============================================================

    private void PrepareSCoFi(PendingPublish pending)
    {
        // Nếu sCoFi được Request() với payload
        // thì pending.Payload đã có sẵn.
    }

    // ============================================================
    // SUCCESS CALLBACK - MePDV
    // ============================================================

    private Task OnMePDVSuccessAsync(PendingPublish pending)
    {
        return Task.CompletedTask;
    }

    // ============================================================
    // SUCCESS CALLBACK - OPERA
    // ============================================================

    private Task OnOperaSuccessAsync(PendingPublish pending)
    {
        if (pending.Data is List<long> messageIds && messageIds.Count > 0)
        {
            _database.MarkMqttSent(messageIds);

            _logger.LogInformation(
                "MQTT Opera database batch marked as sent | "
                    + "Count={Count} | "
                    + "MessageIds={MessageIds}",
                messageIds.Count,
                string.Join(", ", messageIds)
            );
        }

        return Task.CompletedTask;
    }

    // ============================================================
    // SUCCESS CALLBACK - sCoFi
    // ============================================================

    private Task OnSCoFiSuccessAsync(PendingPublish pending)
    {
        return Task.CompletedTask;
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
    // DISPOSE
    // ============================================================

    public override void Dispose()
    {
        _publishSignal.Dispose();

        base.Dispose();
    }

    // ============================================================
    // INNER TYPES
    // ============================================================

    private sealed class PendingPublish
    {
        public string Topic { get; set; } = string.Empty;

        public string? Payload { get; set; }

        // Opera:
        // List<long> = danh sách message_id của batch.
        //
        // Các topic khác có thể sử dụng kiểu dữ liệu khác.
        public object? Data { get; set; }

        public int ConsecutiveFailures { get; set; }
    }

    private sealed class PublishDefinition
    {
        public required Action<PendingPublish> Prepare { get; init; }

        public required Func<PendingPublish, Task> OnSuccessAsync { get; init; }

        public bool PrepareFromDatabase { get; init; }
    }
}
