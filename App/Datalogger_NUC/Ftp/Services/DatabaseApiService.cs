using System.Net.Http.Json;
using Ftp.Models;

namespace Ftp.Services;

public sealed class DatabaseApiService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<DatabaseApiService> _logger;

    public DatabaseApiService(
        IHttpClientFactory httpClientFactory,
        ILogger<DatabaseApiService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    // ============================================================
    // FTP1 - GET PENDING
    // ============================================================

    public async Task<List<StationMessage>> GetPendingFtpMessagesAsync(
        IEnumerable<string>? stationNames,
        int maxMessages,
        CancellationToken cancellationToken)
    {
        string url = BuildPendingUrl(
            "/api/ftp/pending",
            stationNames,
            maxMessages);

        try
        {
            HttpClient client =
                _httpClientFactory.CreateClient("DatabaseApi");

            List<StationMessage>? messages =
                await client.GetFromJsonAsync<List<StationMessage>>(
                    url,
                    cancellationToken);

            return messages ?? new List<StationMessage>();
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Lấy FTP pending API thất bại. Url={Url}",
                url);

            return new List<StationMessage>();
        }
    }

    // ============================================================
    // FTP2 - GET PENDING
    // ============================================================

    public async Task<List<StationMessage>> GetPendingFtp2MessagesAsync(
        IEnumerable<string>? stationNames,
        int maxMessages,
        CancellationToken cancellationToken)
    {
        string url = BuildPendingUrl(
            "/api/ftp2/pending",
            stationNames,
            maxMessages);

        try
        {
            HttpClient client =
                _httpClientFactory.CreateClient("DatabaseApi");

            List<StationMessage>? messages =
                await client.GetFromJsonAsync<List<StationMessage>>(
                    url,
                    cancellationToken);

            return messages ?? new List<StationMessage>();
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Lấy FTP2 pending API thất bại. Url={Url}",
                url);

            return new List<StationMessage>();
        }
    }

    // ============================================================
    // FTP1 - MARK SENT
    // ============================================================

    public async Task MarkFtpSentAsync(
        IEnumerable<long> messageIds,
        CancellationToken cancellationToken)
    {
        await MarkSentAsync(
            "/api/ftp/mark-sent",
            messageIds,
            cancellationToken);
    }

    // ============================================================
    // FTP2 - MARK SENT
    // ============================================================

    public async Task MarkFtp2SentAsync(
        IEnumerable<long> messageIds,
        CancellationToken cancellationToken)
    {
        await MarkSentAsync(
            "/api/ftp2/mark-sent",
            messageIds,
            cancellationToken);
    }

    // ============================================================
    // BUILD PENDING URL
    // ============================================================

    private static string BuildPendingUrl(
        string endpoint,
        IEnumerable<string>? stationNames,
        int maxMessages)
    {
        string url =
            $"{endpoint}?maxMessages={maxMessages}";

        if (stationNames != null)
        {
            string stations =
                string.Join(
                    ",",
                    stationNames.Where(
                        x => !string.IsNullOrWhiteSpace(x)));

            if (!string.IsNullOrWhiteSpace(stations))
            {
                url +=
                    $"&stationNames={Uri.EscapeDataString(stations)}";
            }
        }

        return url;
    }

    // ============================================================
    // MARK SENT
    // ============================================================

    private async Task MarkSentAsync(
        string endpoint,
        IEnumerable<long> messageIds,
        CancellationToken cancellationToken)
    {
        List<long> ids =
            messageIds
                .Distinct()
                .ToList();

        if (ids.Count == 0)
            return;

        HttpClient client =
            _httpClientFactory.CreateClient("DatabaseApi");

        var request =
            new MarkMessagesSentRequest
            {
                Ids = ids
            };

        try
        {
            HttpResponseMessage response =
                await client.PostAsJsonAsync(
                    endpoint,
                    request,
                    cancellationToken);

            response.EnsureSuccessStatusCode();

            _logger.LogInformation(
                "API phản hồi thành công. Endpoint={Endpoint}, Count={Count}",
                endpoint,
                ids.Count);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "API mark-sent thất bại. Endpoint={Endpoint}",
                endpoint);

            throw;
        }
    }
}