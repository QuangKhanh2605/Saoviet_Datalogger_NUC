using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using Mqtt.Configuration;
using Mqtt.Mqtt.Core;
using Mqtt.Mqtt.Models;
using Microsoft.Extensions.Options;

namespace Mqtt.Mqtt.Publishing;

public sealed class MqttPublishManager : BackgroundService
{
    // ============================================================
    // CẤU HÌNH
    // ============================================================

    private const int PublishDelayMilliseconds = 500;
    private const int MaxPublishFailures = 5;
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

    // HTTP client gọi Database API
    private readonly HttpClient _databaseApi;

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
        IHttpClientFactory httpClientFactory,
        IOptions<AppSettings> options)
    {
        _logger = logger;
        _publisher = publisher;
        _connection = connection;

        _databaseApi =
            httpClientFactory.CreateClient("DatabaseApi");

        _settings = options.Value;

        _definitions = new Dictionary<string, PublishDefinition>
        {
            {
                MqttTopics.MePDV,
                new PublishDefinition
                {
                    Prepare = PrepareMePDV,
                    PrepareFromDatabase = false,
                    OnSuccessAsync = OnMePDVSuccessAsync
                }
            },

            {
                MqttTopics.Opera,
                new PublishDefinition
                {
                    Prepare = _ => { },
                    PrepareAsync = PrepareOperaAsync,
                    PrepareFromDatabase = true,
                    OnSuccessAsync = OnOperaSuccessAsync
                }
            },

            {
                MqttTopics.sCoFi,
                new PublishDefinition
                {
                    Prepare = PrepareSCoFi,
                    PrepareFromDatabase = false,
                    OnSuccessAsync = OnSCoFiSuccessAsync
                }
            },

            {
                MqttTopics.sSet1,
                new PublishDefinition
                {
                    Prepare = _ => { },
                    PrepareFromDatabase = false,
                    OnSuccessAsync = OnSSet1SuccessAsync
                }
            }
        };
    }

    // ============================================================
    // REQUEST
    // ============================================================

    public void Request(
        string topic,
        string? payload = null)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException(
                "MQTT topic cannot be empty.",
                nameof(topic));
        }

        lock (_lock)
        {
            _pending[topic] = new PendingPublish
            {
                Topic = topic,
                Payload = payload,
                Data = null,
                ConsecutiveFailures = 0
            };

            if (_publishSignal.CurrentCount == 0)
            {
                _publishSignal.Release();
            }
        }
    }

    // ============================================================
    // BACKGROUND LOOP
    // ============================================================

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "MQTT PublishManager started.");

        try
        {
            await RunAsync(stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Application shutdown bình thường.
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "MQTT PublishManager stopped because of unexpected error.");
        }

        _logger.LogInformation(
            "MQTT PublishManager stopped.");
    }

    // ============================================================
    // MAIN LOOP
    // ============================================================

    public async Task RunAsync(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // ----------------------------------------------------
            // Chờ MQTT READY
            // ----------------------------------------------------

            if (!_connection.IsReady)
            {
                await Task.Delay(
                    500,
                    cancellationToken);

                continue;
            }

            // ----------------------------------------------------
            // Kiểm tra Database API định kỳ
            // ----------------------------------------------------

            await _publishSignal.WaitAsync(
                TimeSpan.FromMilliseconds(500),
                cancellationToken);

            await PrepareDatabaseTopicsAsync(
                cancellationToken);

            // ----------------------------------------------------
            // Xử lý pending MQTT publish
            // ----------------------------------------------------

            await ProcessPendingAsync(
                cancellationToken);
        }
    }

    // ============================================================
    // PROCESS PENDING
    // ============================================================

    private async Task ProcessPendingAsync(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            PendingPublish? pending =
                GetNextPending();

            if (pending == null)
            {
                return;
            }

            if (!_definitions.TryGetValue(
                    pending.Topic,
                    out PublishDefinition? definition))
            {
                _logger.LogWarning(
                    "MQTT topic has no publish definition | Topic={Topic}",
                    pending.Topic);

                RemovePending(pending.Topic);

                continue;
            }

            // ----------------------------------------------------
            // Chuẩn bị payload
            // ----------------------------------------------------

            if (pending.Payload == null)
            {
                if (definition.PrepareFromDatabase &&
                    definition.PrepareAsync != null)
                {
                    await definition.PrepareAsync(
                        pending,
                        cancellationToken);
                }
                else
                {
                    definition.Prepare(pending);
                }
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

            MqttPublishRequest request = new()
            {
                Topic = pending.Topic,
                Payload = pending.Payload
            };

            bool success =
                await _publisher.PublishAsync(
                    request,
                    cancellationToken);

            // ----------------------------------------------------
            // Publish thất bại
            // ----------------------------------------------------

            if (!success)
            {
                await HandlePublishFailureAsync(
                    pending,
                    cancellationToken);

                return;
            }

            // ----------------------------------------------------
            // Publish thành công
            // ----------------------------------------------------

            pending.ConsecutiveFailures = 0;

            bool callbackSuccess =
                await definition.OnSuccessAsync(
                    pending,
                    cancellationToken);

            // Nếu Database API mark-sent thất bại,
            // giữ nguyên pending để không mất batch.
            if (!callbackSuccess)
            {
                _logger.LogWarning(
                    "MQTT {Topic} published, but success callback failed. "
                    + "Keeping pending batch.",
                    pending.Topic);

                await Task.Delay(
                    PublishDelayMilliseconds,
                    cancellationToken);

                return;
            }

            _logger.LogInformation(
                "MQTT {Topic} published successfully.",
                pending.Topic);

            RemovePending(pending.Topic);

            // ----------------------------------------------------
            // Lấy batch tiếp theo từ Database API
            // ----------------------------------------------------

            await PrepareDatabaseTopicsAsync(
                cancellationToken);

            // ----------------------------------------------------
            // Delay trước batch tiếp theo
            // ----------------------------------------------------

            if (HasPending())
            {
                await Task.Delay(
                    PublishDelayMilliseconds,
                    cancellationToken);
            }
        }
    }

    // ============================================================
    // HANDLE PUBLISH FAILURE
    // ============================================================

    private async Task HandlePublishFailureAsync(
        PendingPublish pending,
        CancellationToken cancellationToken)
    {
        pending.ConsecutiveFailures++;

        _logger.LogWarning(
            "MQTT publish failed | Topic={Topic} | Failure={Failure}/{Max}",
            pending.Topic,
            pending.ConsecutiveFailures,
            MaxPublishFailures);

        // Chưa đủ 5 lần:
        // giữ nguyên payload để retry.
        if (pending.ConsecutiveFailures < MaxPublishFailures)
        {
            await Task.Delay(
                PublishDelayMilliseconds,
                cancellationToken);

            return;
        }

        // Đủ 5 lần:
        // yêu cầu reconnect.
        //
        // Không xóa pending.
        pending.ConsecutiveFailures = 0;

        _logger.LogWarning(
            "MQTT publish failed {Max} consecutive times. "
            + "Requesting reconnect | Topic={Topic}",
            MaxPublishFailures,
            pending.Topic);

        _connection.RequestReconnect(
            switchServer: true);
    }

    // ============================================================
    // PREPARE DATABASE TOPICS
    // ============================================================

    private async Task PrepareDatabaseTopicsAsync(
        CancellationToken cancellationToken)
    {
        foreach (string topic in PublishPriority)
        {
            if (!_definitions.TryGetValue(
                    topic,
                    out PublishDefinition? definition))
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
                ConsecutiveFailures = 0
            };

            if (definition.PrepareAsync != null)
            {
                await definition.PrepareAsync(
                    pending,
                    cancellationToken);
            }
            else
            {
                definition.Prepare(pending);
            }

            // Không có dữ liệu Database API.
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

    private void PrepareMePDV(
        PendingPublish pending)
    {
        var data = new
        {
            SaveIntervalMinutes =
                _settings.Database.SaveIntervalMinutes,

            Ver =
                _settings.Device.Ver,

            Server =
                _connection.CurrentServerIp
        };

        pending.Payload =
            JsonSerializer.Serialize(data);

        _logger.LogDebug(
            "MQTT MePDV prepared | Server={Server}",
            _connection.CurrentServerIp);
    }

    // ============================================================
    // PREPARE OPERA
    // ============================================================

    private async Task PrepareOperaAsync(
        PendingPublish pending,
        CancellationToken cancellationToken)
    {
        if (pending.Payload != null)
        {
            return;
        }

        // --------------------------------------------------------
        // Gọi Database API
        //
        // GET /api/mqtt/pending?maxMessages=10
        // --------------------------------------------------------

        try
        {
            string url =
                $"api/mqtt/pending?maxMessages={DatabaseBatchSize}";

            List<StationMessageDto>? messages =
                await _databaseApi.GetFromJsonAsync<
                    List<StationMessageDto>>(
                        url,
                        cancellationToken);

            if (messages == null ||
                messages.Count == 0)
            {
                _logger.LogDebug(
                    "MQTT Opera Database API check | "
                    + "No pending messages.");

                return;
            }

            // ----------------------------------------------------
            // Đóng gói payload Opera
            //
            // Không đưa:
            // Id
            // MqttSent
            // FtpSent
            // Ftp2Sent
            //
            // vào MQTT payload.
            // ----------------------------------------------------

            var data = messages
                .Select(message => new
                {
                    StationName =
                        message.StationName,

                    Timestamp =
                        message.Timestamp,

                    Measurements =
                        message.Measurements
                            .Select(measurement => new
                            {
                                measurement.ParameterName,
                                measurement.Value,
                                measurement.Unit,
                                measurement.Status
                            })
                            .ToList()
                })
                .ToList();

            pending.Payload =
                JsonSerializer.Serialize(data);

            // ----------------------------------------------------
            // Lưu Id của batch
            //
            // Sau khi MQTT publish thành công,
            // gọi POST /api/mqtt/mark-sent
            // ----------------------------------------------------

            pending.Data =
                messages
                    .Select(message => message.Id)
                    .ToList();

            _logger.LogDebug(
                "MQTT Opera prepared from Database API | "
                + "BatchSize={BatchSize} | Stations={Stations}",
                messages.Count,
                string.Join(
                    ", ",
                    messages.Select(x => x.StationName)));
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Database API pending request failed.");

            // Không tạo payload.
            // Lần sau sẽ gọi lại API.
        }
    }

    // ============================================================
    // PREPARE sCoFi
    // ============================================================

    private void PrepareSCoFi(
        PendingPublish pending)
    {
        // Nếu sCoFi được Request() với payload
        // thì pending.Payload đã có sẵn.
    }

    // ============================================================
    // SUCCESS CALLBACK - MePDV
    // ============================================================

    private Task<bool> OnMePDVSuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(true);
    }

    // ============================================================
    // SUCCESS CALLBACK - OPERA
    // ============================================================

    private async Task<bool> OnOperaSuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken)
    {
        if (pending.Data is not List<long> messageIds ||
            messageIds.Count == 0)
        {
            return true;
        }

        // --------------------------------------------------------
        // MQTT publish thành công.
        //
        // Gọi Database API:
        //
        // POST /api/mqtt/mark-sent
        //
        // Body:
        //
        // {
        //     "Ids": [101, 102, 103]
        // }
        // --------------------------------------------------------

        try
        {
            MarkMessagesSentRequest request = new()
            {
                Ids = messageIds
            };

            HttpResponseMessage response =
                await _databaseApi.PostAsJsonAsync(
                    "api/mqtt/mark-sent",
                    request,
                    cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "Database API mark-sent failed | "
                    + "StatusCode={StatusCode} | "
                    + "MessageIds={MessageIds}",
                    (int)response.StatusCode,
                    string.Join(
                        ", ",
                        messageIds));

                return false;
            }

            _logger.LogInformation(
                "MQTT Opera Database API batch marked as sent | "
                + "Count={Count} | MessageIds={MessageIds}",
                messageIds.Count,
                string.Join(
                    ", ",
                    messageIds));

            return true;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Database API mark-sent request failed | "
                + "MessageIds={MessageIds}",
                string.Join(
                    ", ",
                    messageIds));

            return false;
        }
    }

    // ============================================================
    // SUCCESS CALLBACK - sCoFi
    // ============================================================

    private Task<bool> OnSCoFiSuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(true);
    }

    // ============================================================
    // SUCCESS CALLBACK - sSet1
    // ============================================================

    private Task<bool> OnSSet1SuccessAsync(
        PendingPublish pending,
        CancellationToken cancellationToken)
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
                if (_pending.TryGetValue(
                        topic,
                        out PendingPublish? pending))
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
        // Danh sách Id của batch.
        public object? Data { get; set; }

        public int ConsecutiveFailures { get; set; }
    }

    private sealed class PublishDefinition
    {
        public required
            Action<PendingPublish> Prepare
        {
            get;
            init;
        }

        public Func<
            PendingPublish,
            CancellationToken,
            Task>? PrepareAsync
        {
            get;
            init;
        }

        public required
            Func<
                PendingPublish,
                CancellationToken,
                Task<bool>>
            OnSuccessAsync
        {
            get;
            init;
        }

        public bool PrepareFromDatabase
        {
            get;
            init;
        }
    }

    // ============================================================
    // DATABASE API DTO
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

        public string ParameterName { get; set; } = "";

        public double Value { get; set; }

        public string Unit { get; set; } = "";

        public int Status { get; set; }

        public string Timestamp { get; set; } = "";
    }

    private sealed class MarkMessagesSentRequest
    {
        public List<long> Ids { get; set; } = new();
    }
}