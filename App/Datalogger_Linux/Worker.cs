// using Datalogger_Linux.Workers;

// namespace Datalogger_Linux;

// public class Worker : BackgroundService
// {
//     private readonly ILogger<Worker> _logger;

//     private readonly ModbusWorker _modbusWorker;

//     private readonly DatabaseWorker _databaseWorker;

//     private readonly FtpWorker _ftpWorker;

//     public Worker(
//         ILogger<Worker> logger,
//         ModbusWorker modbusWorker,
//         DatabaseWorker databaseWorker,
//         FtpWorker ftpWorker
//     )
//     {
//         _logger = logger;

//         _modbusWorker = modbusWorker;

//         _databaseWorker = databaseWorker;

//         _ftpWorker = ftpWorker;
//     }

//     protected override async Task ExecuteAsync(CancellationToken stoppingToken)
//     {
//         _logger.LogInformation("Datalogger Worker started.");

//         try
//         {
//             Task modbusTask = _modbusWorker.RunAsync(stoppingToken);

//             Task databaseTask = _databaseWorker.RunAsync(stoppingToken);

//             Task ftpTask = _ftpWorker.RunAsync(stoppingToken);

//             await Task.WhenAll(modbusTask, databaseTask, ftpTask);
//         }
//         catch (OperationCanceledException)
//         {
//             _logger.LogInformation("Datalogger Worker stopping.");
//         }
//         catch (Exception ex)
//         {
//             _logger.LogError(ex, "Datalogger Worker stopped unexpectedly.");
//         }

//         _logger.LogInformation("Datalogger Worker stopped.");
//     }
// }
