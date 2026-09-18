using Datalogger_Linux;
using Datalogger_Linux.Configuration;
using Datalogger_Linux.Decoders;
using Datalogger_Linux.Ftp;
using Datalogger_Linux.Hardware;
using Datalogger_Linux.Mqtt;
using Datalogger_Linux.Mqtt.Core;
using Datalogger_Linux.Mqtt.Publishing;
using Datalogger_Linux.Mqtt.Subscribing;
using Datalogger_Linux.Sensors;
using Datalogger_Linux.Services;
using Datalogger_Linux.Workers;

var builder = Host.CreateApplicationBuilder(args);

// =============================================================
// RUNTIME CONFIGURATION
// =============================================================

builder.Configuration.AddJsonFile(
    RuntimePaths.AppSettings,
    optional: false,
    reloadOnChange: true);

builder.Services.Configure<AppSettings>(builder.Configuration);

// =============================================================
// CORE
// =============================================================

builder.Services.AddSingleton<DatabaseService>();
builder.Services.AddSingleton<ReadingStore>();
builder.Services.AddSingleton<DecoderFactory>();

// =============================================================
// STATION CONFIGURATION
// =============================================================

builder.Services.AddSingleton<StationConfigurationService>(serviceProvider =>
{
    return new StationConfigurationService(
        RuntimePaths.Stations);
});

builder.Services.AddSingleton<StationReader>();
builder.Services.AddSingleton<StationMessageService>();

// =============================================================
// FTP
// =============================================================

builder.Services.AddSingleton<FtpService>();
builder.Services.AddHostedService<FtpWorker>();

// =============================================================
// MQTT CORE
// =============================================================

builder.Services.AddSingleton<MqttService>();
builder.Services.AddSingleton<MqttConnectionManager>();
builder.Services.AddSingleton<MqttTopicBuilder>();

// =============================================================
// MQTT PUBLISHING
// =============================================================

builder.Services.AddSingleton<MqttPublisher>();
builder.Services.AddSingleton<MqttPublishManager>();

// =============================================================
// MQTT SUBSCRIBING
// =============================================================

builder.Services.AddSingleton<MqttSubscriber>();

// =============================================================
// MQTT WORKER
// =============================================================

builder.Services.AddHostedService<MqttWorker>();

// =============================================================
// OTHER WORKERS
// =============================================================

builder.Services.AddHostedService<ModbusWorker>();
builder.Services.AddHostedService<DatabaseWorker>();

// =============================================================
// BUILD HOST
// =============================================================

var host = builder.Build();

// =============================================================
// DATABASE
// =============================================================

DatabaseService database =
    host.Services.GetRequiredService<DatabaseService>();

database.Initialize();

// =============================================================
// RUN
// =============================================================

host.Run();