using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Modbus.Configuration;
using Modbus.Services;
using Modbus.Workers;

namespace Modbus;

public static class ModbusApi
{
    public static void Map(WebApplication app)
    {
        // ============================================================
        // GET ALL READINGS
        // ============================================================

        app.MapGet(
            "/api/readings",
            ([FromServices] ReadingStore readingStore) =>
            {
                var readings = readingStore
                    .GetLatestReadings()
                    .Select(x => new
                    {
                        x.StationName,
                        x.Name,
                        x.Value,
                        x.Unit,
                        x.Status,
                        x.Timestamp,
                    })
                    .ToList();

                return Results.Ok(readings);
            }
        );

        app.MapGet(
            "/api/readings/{stationName}",
            (string stationName, [FromServices] ReadingStore readingStore) =>
            {
                var readings = readingStore
                    .GetLatestReadings()
                    .Where(x =>
                        string.Equals(
                            x.StationName,
                            stationName,
                            StringComparison.OrdinalIgnoreCase
                        )
                    )
                    .Select(x => new
                    {
                        x.StationName,
                        x.Name,
                        x.Value,
                        x.Unit,
                        x.Status,
                        x.Timestamp,
                    })
                    .ToList();

                return Results.Ok(readings);
            }
        );

        // ============================================================
        // HEALTH
        // ============================================================

        app.MapGet("/api/health", () => Results.Ok(new { Service = "Modbus", Status = "Running" }));

        // ============================================================
        // GET RAW STATION CONFIGURATION
        // ============================================================

        app.MapGet(
            "/api/config/stations",
            ([FromServices] StationConfigurationService configurationService) =>
            {
                try
                {
                    string json = configurationService.ReadRaw();

                    return Results.Text(json, contentType: "application/json");
                }
                catch (FileNotFoundException ex)
                {
                    return Results.NotFound(new { Error = ex.Message });
                }
                catch (IOException ex)
                {
                    return Results.Problem(
                        title: "Không đọc được station.json.",
                        detail: ex.Message
                    );
                }
            }
        );

        // ============================================================
        // PUT RAW STATION CONFIGURATION + REQUEST RELOAD
        // ============================================================

        app.MapPut(
            "/api/config/stations",
            async (
                HttpRequest request,
                [FromServices] StationConfigurationService configurationService,
                [FromServices] ModbusWorker modbusWorker
            ) =>
            {
                string json;

                using (var reader = new StreamReader(request.Body, Encoding.UTF8))
                {
                    json = await reader.ReadToEndAsync();
                }

                try
                {
                    // Xác thực cấu hình trước khi thay file gốc.
                    configurationService.SaveRaw(json);
                }
                catch (JsonException ex)
                {
                    return Results.BadRequest(
                        new { Error = "JSON không hợp lệ.", Detail = ex.Message }
                    );
                }
                catch (InvalidDataException ex)
                {
                    return Results.BadRequest(new { Error = ex.Message });
                }
                catch (FormatException ex)
                {
                    return Results.BadRequest(
                        new { Error = "Cấu hình có địa chỉ không hợp lệ.", Detail = ex.Message }
                    );
                }
                catch (ArgumentException ex)
                {
                    return Results.BadRequest(
                        new { Error = "Cấu hình không hợp lệ.", Detail = ex.Message }
                    );
                }
                catch (NotSupportedException ex)
                {
                    return Results.BadRequest(
                        new { Error = "Cấu hình không được hỗ trợ.", Detail = ex.Message }
                    );
                }
                catch (IOException ex)
                {
                    return Results.Problem(
                        title: "Không thể lưu station.json.",
                        detail: ex.Message
                    );
                }

                // Chỉ yêu cầu reload sau khi lưu thành công.
                modbusWorker.RequestReload();

                return Results.Ok(
                    new { Message = "Đã lưu cấu hình. Modbus Worker đang tải lại cấu hình." }
                );
            }
        );

        // ============================================================
        // GET RAW MODBUS CONFIGURATION
        // ============================================================

        app.MapGet(
            "/api/config/modbus",
            () =>
            {
                try
                {
                    string path = RuntimePaths.AppSettings;

                    if (!File.Exists(path))
                    {
                        return Results.NotFound(
                            new { Error = "Không tìm thấy ModbusConfig.json.", Path = path }
                        );
                    }

                    string json = File.ReadAllText(path);

                    return Results.Text(json, contentType: "application/json");
                }
                catch (IOException ex)
                {
                    return Results.Problem(
                        title: "Không đọc được ModbusConfig.json.",
                        detail: ex.Message
                    );
                }
            }
        );

        // ============================================================
        // PUT RAW MODBUS CONFIGURATION + REQUEST RELOAD
        // ============================================================

        app.MapPut(
            "/api/config/modbus",
            async (HttpRequest request, [FromServices] ModbusWorker modbusWorker) =>
            {
                string json;

                using (var reader = new StreamReader(request.Body, Encoding.UTF8))
                {
                    json = await reader.ReadToEndAsync();
                }

                string path = RuntimePaths.AppSettings;
                string? tempPath = null;

                try
                {
                    // Kiểm tra JSON hợp lệ và có cấu trúc object.
                    using JsonDocument document = JsonDocument.Parse(json);

                    if (document.RootElement.ValueKind != JsonValueKind.Object)
                    {
                        return Results.BadRequest(
                            new { Error = "ModbusConfig.json phải có cấu trúc JSON object." }
                        );
                    }

                    string? directory = Path.GetDirectoryName(path);

                    if (!string.IsNullOrEmpty(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }

                    // Ghi file tạm trước khi thay file chính.
                    tempPath = path + "." + Guid.NewGuid().ToString("N") + ".tmp";

                    await File.WriteAllTextAsync(tempPath, json);

                    File.Move(tempPath, path, overwrite: true);

                    // Gửi tín hiệu reload sau khi lưu thành công.
                    modbusWorker.RequestReload();

                    return Results.Ok(
                        new
                        {
                            Success = true,
                            Message = "Đã lưu ModbusConfig.json và gửi yêu cầu reload.",
                            Path = path,
                        }
                    );
                }
                catch (JsonException ex)
                {
                    return Results.BadRequest(
                        new { Error = "JSON không hợp lệ.", Detail = ex.Message }
                    );
                }
                catch (IOException ex)
                {
                    return Results.Problem(
                        title: "Không thể lưu ModbusConfig.json.",
                        detail: ex.Message
                    );
                }
                finally
                {
                    if (tempPath is not null && File.Exists(tempPath))
                    {
                        try
                        {
                            File.Delete(tempPath);
                        }
                        catch (IOException)
                        {
                            // Không làm thay đổi kết quả API nếu
                            // không xóa được file tạm.
                        }
                    }
                }
            }
        );
    }
}
