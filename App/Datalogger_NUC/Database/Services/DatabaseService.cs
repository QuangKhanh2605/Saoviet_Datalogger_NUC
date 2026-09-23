using Database.Configuration;
using Database.Models;
using Microsoft.Data.Sqlite;

namespace Database.Services;

public sealed class DatabaseService
{
    private readonly string _connectionString;
    private readonly string _databasePath;

    public DatabaseService()
    {
        Directory.CreateDirectory(RuntimePaths.Database);

        _databasePath = RuntimePaths.DatabaseFile;
        _connectionString = $"Data Source={_databasePath}";

        Console.WriteLine($"SQLite Database: {_databasePath}");
    }

    // =========================================================
    // CONNECTION
    // =========================================================

    private SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(_connectionString);

        connection.Open();

        using var command = connection.CreateCommand();

        command.CommandText = """
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            """;

        command.ExecuteNonQuery();

        return connection;
    }

    // =========================================================
    // INITIALIZE DATABASE
    // =========================================================

    public void Initialize()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
            CREATE TABLE IF NOT EXISTS station_messages
            (
                message_id   INTEGER PRIMARY KEY,
                timestamp    TEXT NOT NULL,
                station_name TEXT NOT NULL,
                mqtt_sent    INTEGER NOT NULL DEFAULT 0,
                ftp_sent     INTEGER NOT NULL DEFAULT 0,
                ftp2_sent    INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS measurements
            (
                message_id INTEGER NOT NULL,
                name       TEXT NOT NULL,
                value      REAL NOT NULL,
                unit       TEXT NOT NULL,
                status     INTEGER NOT NULL,

                FOREIGN KEY(message_id)
                    REFERENCES station_messages(message_id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_station_time
                ON station_messages(station_name, timestamp);

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_timestamp
                ON station_messages(timestamp, message_id);

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_mqtt
                ON station_messages(mqtt_sent, message_id);

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_ftp
                ON station_messages(ftp_sent, message_id);

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_ftp2
                ON station_messages(ftp2_sent, message_id);

            CREATE INDEX IF NOT EXISTS
                idx_measurements_message
                ON measurements(message_id);
            """;

        command.ExecuteNonQuery();

        Console.WriteLine("SQLite database initialized.");
    }

    // =========================================================
    // GET MAX MESSAGE ID
    // =========================================================

    public long GetMaxMessageId()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
            SELECT COALESCE(MAX(message_id), 0)
            FROM station_messages;
            """;

        return Convert.ToInt64(command.ExecuteScalar());
    }

    // =========================================================
    // SAVE MQTT
    // =========================================================

    public void SaveMqttMessages(IEnumerable<StationMessage> messages)
    {
        SaveQueueMessages(messages, "mqtt_sent");
    }

    // =========================================================
    // SAVE FTP
    // =========================================================

    public void SaveFtpMessages(IEnumerable<StationMessage> messages)
    {
        SaveQueueMessages(messages, "ftp_sent");
    }

    // =========================================================
    // SAVE FTP2
    // =========================================================

    public void SaveFtp2Messages(IEnumerable<StationMessage> messages)
    {
        SaveQueueMessages(messages, "ftp2_sent");
    }

    // =========================================================
    // SAVE QUEUE MESSAGES
    // =========================================================

    private void SaveQueueMessages(IEnumerable<StationMessage> messages, string sentColumn)
    {
        if (sentColumn is not ("mqtt_sent" or "ftp_sent" or "ftp2_sent"))
        {
            throw new ArgumentException("Invalid sent column.", nameof(sentColumn));
        }

        List<StationMessage> messageList = messages.ToList();

        if (messageList.Count == 0)
        {
            return;
        }

        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            using var existsCommand = connection.CreateCommand();

            existsCommand.Transaction = transaction;

            existsCommand.CommandText = """
                SELECT EXISTS
                (
                    SELECT 1
                    FROM station_messages
                    WHERE message_id = $message_id
                );
                """;

            var existsParameter = existsCommand.Parameters.Add("$message_id", SqliteType.Integer);

            using var insertCommand = connection.CreateCommand();

            insertCommand.Transaction = transaction;

            insertCommand.CommandText = """
                INSERT INTO station_messages
                (
                    message_id,
                    timestamp,
                    station_name,
                    mqtt_sent,
                    ftp_sent,
                    ftp2_sent
                )
                VALUES
                (
                    $message_id,
                    $timestamp,
                    $station_name,
                    $mqtt_sent,
                    $ftp_sent,
                    $ftp2_sent
                );
                """;

            var insertId = insertCommand.Parameters.Add("$message_id", SqliteType.Integer);

            var insertTimestamp = insertCommand.Parameters.Add("$timestamp", SqliteType.Text);

            var insertStation = insertCommand.Parameters.Add("$station_name", SqliteType.Text);

            var insertMqtt = insertCommand.Parameters.Add("$mqtt_sent", SqliteType.Integer);

            var insertFtp = insertCommand.Parameters.Add("$ftp_sent", SqliteType.Integer);

            var insertFtp2 = insertCommand.Parameters.Add("$ftp2_sent", SqliteType.Integer);

            using var updateCommand = connection.CreateCommand();

            updateCommand.Transaction = transaction;

            updateCommand.CommandText = $"""
                UPDATE station_messages
                SET {sentColumn} = 1
                WHERE message_id = $message_id;
                """;

            var updateId = updateCommand.Parameters.Add("$message_id", SqliteType.Integer);

            foreach (StationMessage message in messageList)
            {
                ValidateMessage(message);

                if (message.Id <= 0)
                {
                    throw new ArgumentException(
                        "Message ID must be generated before SQLite persistence."
                    );
                }

                existsParameter.Value = message.Id;

                bool exists = Convert.ToInt32(existsCommand.ExecuteScalar()) != 0;

                if (exists)
                {
                    bool sent = sentColumn switch
                    {
                        "mqtt_sent" => message.MqttSent,
                        "ftp_sent" => message.FtpSent,
                        "ftp2_sent" => message.Ftp2Sent,
                        _ => false,
                    };

                    // Không bao giờ reset TRUE -> FALSE.
                    if (sent)
                    {
                        updateId.Value = message.Id;
                        updateCommand.ExecuteNonQuery();
                    }

                    continue;
                }

                insertId.Value = message.Id;
                insertTimestamp.Value = message.Timestamp;
                insertStation.Value = message.StationName;

                insertMqtt.Value = message.MqttSent ? 1 : 0;
                insertFtp.Value = message.FtpSent ? 1 : 0;
                insertFtp2.Value = message.Ftp2Sent ? 1 : 0;

                insertCommand.ExecuteNonQuery();

                InsertMeasurements(connection, transaction, message);
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    // =========================================================
    // INSERT MEASUREMENTS
    // =========================================================

    private static void InsertMeasurements(
        SqliteConnection connection,
        SqliteTransaction transaction,
        StationMessage message
    )
    {
        if (message.Measurements.Count == 0)
        {
            return;
        }

        using var command = connection.CreateCommand();

        command.Transaction = transaction;

        command.CommandText = """
            INSERT INTO measurements
            (
                message_id,
                name,
                value,
                unit,
                status
            )
            VALUES
            (
                $message_id,
                $name,
                $value,
                $unit,
                $status
            );
            """;

        var messageId = command.Parameters.Add("$message_id", SqliteType.Integer);

        var name = command.Parameters.Add("$name", SqliteType.Text);

        var value = command.Parameters.Add("$value", SqliteType.Real);

        var unit = command.Parameters.Add("$unit", SqliteType.Text);

        var status = command.Parameters.Add("$status", SqliteType.Integer);

        foreach (SensorReading reading in message.Measurements)
        {
            messageId.Value = message.Id;
            name.Value = reading.Name;
            value.Value = reading.Value;
            unit.Value = reading.Unit;
            status.Value = reading.Status;

            command.ExecuteNonQuery();
        }
    }

    // =========================================================
    // VALIDATE MESSAGE
    // =========================================================

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

    // =========================================================
    // PENDING MQTT
    // =========================================================

    public List<StationMessage> GetPendingMqttMessages(int maxMessages = 5)
    {
        if (maxMessages <= 0)
        {
            return [];
        }

        using var connection = OpenConnection();

        List<StationMessage> messages = GetPendingMessages(
            connection,
            "mqtt_sent = 0",
            maxMessages
        );

        LoadMeasurements(connection, messages);

        return messages;
    }

    // =========================================================
    // PENDING FTP
    // =========================================================

    public List<StationMessage> GetPendingFtpMessages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50
    )
    {
        if (maxMessages <= 0)
        {
            return [];
        }

        using var connection = OpenConnection();

        List<StationMessage> messages = GetPendingFtpMessagesInternal(
            connection,
            stationNames,
            ftp2: false,
            maxMessages
        );

        LoadMeasurements(connection, messages);

        return messages;
    }

    // =========================================================
    // PENDING FTP2
    // =========================================================

    public List<StationMessage> GetPendingFtp2Messages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50
    )
    {
        if (maxMessages <= 0)
        {
            return [];
        }

        using var connection = OpenConnection();

        List<StationMessage> messages = GetPendingFtpMessagesInternal(
            connection,
            stationNames,
            ftp2: true,
            maxMessages
        );

        LoadMeasurements(connection, messages);

        return messages;
    }

    // =========================================================
    // GET PENDING MESSAGES
    // =========================================================

    private static List<StationMessage> GetPendingMessages(
        SqliteConnection connection,
        string condition,
        int maxMessages
    )
    {
        using var command = connection.CreateCommand();

        command.CommandText = $"""
            SELECT
                message_id,
                timestamp,
                station_name,
                mqtt_sent,
                ftp_sent,
                ftp2_sent
            FROM station_messages
            WHERE {condition}
            ORDER BY timestamp, message_id
            LIMIT $limit;
            """;

        command.Parameters.AddWithValue("$limit", maxMessages);

        return ReadMessages(command);
    }

    // =========================================================
    // GET PENDING FTP / FTP2
    // =========================================================

    private static List<StationMessage> GetPendingFtpMessagesInternal(
        SqliteConnection connection,
        IEnumerable<string>? stationNames,
        bool ftp2,
        int maxMessages
    )
    {
        string sentColumn = ftp2 ? "ftp2_sent" : "ftp_sent";

        using var command = connection.CreateCommand();

        List<string>? names = stationNames
            ?.Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        string stationCondition = "";

        if (names is { Count: > 0 })
        {
            var parameters = new List<string>(names.Count);

            for (int i = 0; i < names.Count; i++)
            {
                string parameterName = $"$station{i}";

                parameters.Add(parameterName);

                command.Parameters.AddWithValue(parameterName, names[i]);
            }

            stationCondition = $"AND station_name IN ({string.Join(", ", parameters)})";
        }

        command.CommandText = $"""
            SELECT
                message_id,
                timestamp,
                station_name,
                mqtt_sent,
                ftp_sent,
                ftp2_sent
            FROM station_messages
            WHERE {sentColumn} = 0
            {stationCondition}
            ORDER BY timestamp, message_id
            LIMIT $limit;
            """;

        command.Parameters.AddWithValue("$limit", maxMessages);

        return ReadMessages(command);
    }

    // =========================================================
    // READ MESSAGES
    // =========================================================

    private static List<StationMessage> ReadMessages(SqliteCommand command)
    {
        var messages = new List<StationMessage>();

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            messages.Add(
                new StationMessage
                {
                    Id = reader.GetInt64(0),
                    Timestamp = reader.GetString(1),
                    StationName = reader.GetString(2),

                    MqttSent = reader.GetInt64(3) != 0,
                    FtpSent = reader.GetInt64(4) != 0,
                    Ftp2Sent = reader.GetInt64(5) != 0,
                }
            );
        }

        return messages;
    }

    // =========================================================
    // LOAD MEASUREMENTS
    // =========================================================

    private static void LoadMeasurements(SqliteConnection connection, List<StationMessage> messages)
    {
        if (messages.Count == 0)
        {
            return;
        }

        var messageMap = messages.ToDictionary(x => x.Id);

        using var command = connection.CreateCommand();

        var parameters = new List<string>(messages.Count);

        for (int i = 0; i < messages.Count; i++)
        {
            string parameterName = $"$id{i}";

            parameters.Add(parameterName);

            command.Parameters.AddWithValue(parameterName, messages[i].Id);
        }

        command.CommandText = $"""
            SELECT
                m.message_id,
                m.name,
                m.value,
                m.unit,
                m.status,
                sm.station_name,
                sm.timestamp
            FROM measurements m
            INNER JOIN station_messages sm
                ON sm.message_id = m.message_id
            WHERE m.message_id IN
                ({string.Join(", ", parameters)})
            ORDER BY m.message_id;
            """;

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            long messageId = reader.GetInt64(0);

            if (!messageMap.TryGetValue(messageId, out StationMessage? message))
            {
                continue;
            }

            message.Measurements.Add(
                new SensorReading
                {
                    Name = reader.GetString(1),
                    Value = reader.GetDouble(2),
                    Unit = reader.GetString(3),
                    Status = reader.GetInt32(4),
                    StationName = reader.GetString(5),
                    Timestamp = reader.GetString(6),
                }
            );
        }
    }

    // =========================================================
    // MARK MQTT SENT
    // =========================================================

    public void MarkMqttSent(IEnumerable<long> messageIds)
    {
        MarkSent(messageIds, "mqtt_sent");
    }

    // =========================================================
    // MARK FTP SENT
    // =========================================================

    public void MarkFtpSent(IEnumerable<long> messageIds)
    {
        MarkSent(messageIds, "ftp_sent");
    }

    // =========================================================
    // MARK FTP2 SENT
    // =========================================================

    public void MarkFtp2Sent(IEnumerable<long> messageIds)
    {
        MarkSent(messageIds, "ftp2_sent");
    }

    // =========================================================
    // MARK SENT
    // =========================================================

    private void MarkSent(IEnumerable<long> messageIds, string columnName)
    {
        List<long> ids = messageIds.Distinct().ToList();

        if (ids.Count == 0)
        {
            return;
        }

        if (columnName is not ("mqtt_sent" or "ftp_sent" or "ftp2_sent"))
        {
            throw new ArgumentException("Invalid sent column.", nameof(columnName));
        }

        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            using var command = connection.CreateCommand();

            command.Transaction = transaction;

            command.CommandText = $"""
                UPDATE station_messages
                SET {columnName} = 1
                WHERE message_id = $message_id;
                """;

            var parameter = command.Parameters.Add("$message_id", SqliteType.Integer);

            foreach (long id in ids)
            {
                parameter.Value = id;

                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    // =========================================================
    // DATABASE SIZE
    // =========================================================

    private long GetDatabaseSizeBytes()
    {
        long size = 0;

        if (File.Exists(_databasePath))
        {
            size += new FileInfo(_databasePath).Length;
        }

        string walPath = _databasePath + "-wal";

        if (File.Exists(walPath))
        {
            size += new FileInfo(walPath).Length;
        }

        string shmPath = _databasePath + "-shm";

        if (File.Exists(shmPath))
        {
            size += new FileInfo(shmPath).Length;
        }

        return size;
    }

    // =========================================================
    // DELETE EXPIRED DATA
    // =========================================================

    private static int DeleteExpiredData(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int retentionDays
    )
    {
        string cutoff = DateTime.Now.AddDays(-retentionDays).ToString("yyyyMMddHHmmss");

        using var command = connection.CreateCommand();

        command.Transaction = transaction;

        command.CommandText = """
            DELETE FROM station_messages
            WHERE timestamp < $cutoff;
            """;

        command.Parameters.AddWithValue("$cutoff", cutoff);

        return command.ExecuteNonQuery();
    }

    // =========================================================
    // DELETE OLDEST
    // =========================================================

    private static int DeleteOldestMessages(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int batchSize
    )
    {
        using var command = connection.CreateCommand();

        command.Transaction = transaction;

        command.CommandText = """
            DELETE FROM station_messages
            WHERE message_id IN
            (
                SELECT message_id
                FROM station_messages
                ORDER BY timestamp, message_id
                LIMIT $batchSize
            );
            """;

        command.Parameters.AddWithValue("$batchSize", batchSize);

        return command.ExecuteNonQuery();
    }

    // =========================================================
    // CHECKPOINT WAL
    // =========================================================

    private void CheckpointWal()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";

        command.ExecuteNonQuery();
    }

    // =========================================================
    // VACUUM
    // =========================================================

    private void Vacuum()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = "VACUUM;";

        command.ExecuteNonQuery();
    }

    // =========================================================
    // CLEANUP DATABASE
    // =========================================================

    public DatabaseCleanupResult CleanupDatabase(
        int retentionDays,
        int maxDatabaseSizeMB,
        bool vacuumAfterCleanup
    )
    {
        if (retentionDays <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionDays));
        }

        if (maxDatabaseSizeMB <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDatabaseSizeMB));
        }

        long maxBytes = (long)maxDatabaseSizeMB * 1024 * 1024;

        var result = new DatabaseCleanupResult { SizeBeforeBytes = GetDatabaseSizeBytes() };

        bool deleted = false;

        using (var connection = OpenConnection())
        using (var transaction = connection.BeginTransaction())
        {
            try
            {
                result.DeletedByRetention = DeleteExpiredData(
                    connection,
                    transaction,
                    retentionDays
                );

                if (result.DeletedByRetention > 0)
                {
                    deleted = true;
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        while (GetDatabaseSizeBytes() > maxBytes)
        {
            int deletedCount;

            using (var connection = OpenConnection())
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    deletedCount = DeleteOldestMessages(connection, transaction, 1000);

                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }

            if (deletedCount == 0)
            {
                break;
            }

            result.DeletedBySize += deletedCount;

            deleted = true;

            CheckpointWal();
        }

        if (deleted)
        {
            CheckpointWal();

            if (vacuumAfterCleanup)
            {
                Vacuum();
            }
        }

        result.SizeAfterBytes = GetDatabaseSizeBytes();

        return result;
    }
}
