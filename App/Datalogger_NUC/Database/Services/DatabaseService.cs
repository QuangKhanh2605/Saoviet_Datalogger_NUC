using Database.Configuration;
using Database.Models;
using Microsoft.Data.Sqlite;

namespace Database.Services;

public class DatabaseService
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
                message_id   INTEGER PRIMARY KEY AUTOINCREMENT,
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
                idx_measurements_message
                ON measurements(message_id);

            CREATE INDEX IF NOT EXISTS
                idx_measurements_name
                ON measurements(name);

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_ftp2
                ON station_messages(ftp2_sent, message_id);

            CREATE INDEX IF NOT EXISTS
                idx_station_messages_pending
                ON station_messages(
                    mqtt_sent,
                    ftp_sent,
                    ftp2_sent,
                    message_id
                );
            """;

        command.ExecuteNonQuery();

        EnsureFtp2SentColumn(connection);

        Console.WriteLine("SQLite database initialized.");
    }

    // =========================================================
    // MIGRATION
    // =========================================================

    private static void EnsureFtp2SentColumn(
        SqliteConnection connection)
    {
        using var command = connection.CreateCommand();

        command.CommandText = """
            PRAGMA table_info(station_messages);
            """;

        bool exists = false;

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            string columnName = reader.GetString(1);

            if (string.Equals(
                    columnName,
                    "ftp2_sent",
                    StringComparison.OrdinalIgnoreCase))
            {
                exists = true;
                break;
            }
        }

        if (exists)
        {
            return;
        }

        using var alterCommand = connection.CreateCommand();

        alterCommand.CommandText = """
            ALTER TABLE station_messages
            ADD COLUMN ftp2_sent
            INTEGER NOT NULL DEFAULT 0;
            """;

        alterCommand.ExecuteNonQuery();

        Console.WriteLine(
            "SQLite migration: added ftp2_sent column.");
    }

    // =========================================================
    // INSERT
    // =========================================================

    public void InsertStationMessages(
        IEnumerable<StationMessage> messages)
    {
        var messageList = messages.ToList();

        if (messageList.Count == 0)
        {
            return;
        }

        using var connection = OpenConnection();

        using var transaction =
            connection.BeginTransaction();

        try
        {
            foreach (var message in messageList)
            {
                if (string.IsNullOrWhiteSpace(
                        message.StationName))
                {
                    throw new ArgumentException(
                        "StationName cannot be empty.");
                }

                if (string.IsNullOrWhiteSpace(
                        message.Timestamp))
                {
                    throw new ArgumentException(
                        "Timestamp cannot be empty.");
                }

                // -------------------------------------------------
                // INSERT STATION MESSAGE
                // -------------------------------------------------

                using var messageCommand =
                    connection.CreateCommand();

                messageCommand.Transaction = transaction;

                messageCommand.CommandText = """
                    INSERT INTO station_messages
                    (
                        timestamp,
                        station_name,
                        mqtt_sent,
                        ftp_sent,
                        ftp2_sent
                    )
                    VALUES
                    (
                        $timestamp,
                        $station_name,
                        $mqtt_sent,
                        $ftp_sent,
                        $ftp2_sent
                    );

                    SELECT last_insert_rowid();
                    """;

                messageCommand.Parameters.AddWithValue(
                    "$timestamp",
                    message.Timestamp);

                messageCommand.Parameters.AddWithValue(
                    "$station_name",
                    message.StationName);

                messageCommand.Parameters.AddWithValue(
                    "$mqtt_sent",
                    message.MqttSent ? 1 : 0);

                messageCommand.Parameters.AddWithValue(
                    "$ftp_sent",
                    message.FtpSent ? 1 : 0);

                messageCommand.Parameters.AddWithValue(
                    "$ftp2_sent",
                    message.Ftp2Sent ? 1 : 0);

                message.Id =
                    Convert.ToInt64(
                        messageCommand.ExecuteScalar());

                // -------------------------------------------------
                // INSERT MEASUREMENTS
                // -------------------------------------------------

                foreach (var reading in message.Measurements)
                {
                    using var measurementCommand =
                        connection.CreateCommand();

                    measurementCommand.Transaction =
                        transaction;

                    measurementCommand.CommandText = """
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

                    measurementCommand.Parameters.AddWithValue(
                        "$message_id",
                        message.Id);

                    measurementCommand.Parameters.AddWithValue(
                        "$name",
                        reading.ParameterName);

                    measurementCommand.Parameters.AddWithValue(
                        "$value",
                        reading.Value);

                    measurementCommand.Parameters.AddWithValue(
                        "$unit",
                        reading.Unit);

                    measurementCommand.Parameters.AddWithValue(
                        "$status",
                        reading.Status);

                    measurementCommand.ExecuteNonQuery();
                }
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

    public long GetDatabaseSizeBytes()
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
    // PENDING MQTT
    // =========================================================

    public List<StationMessage> GetPendingMqttMessages(
        int maxMessages = 5)
    {
        using var connection = OpenConnection();

        var messages = GetPendingMessages(
            connection,
            """
            mqtt_sent = 0
            """,
            maxMessages);

        LoadMeasurements(
            connection,
            messages);

        return messages;
    }

    // =========================================================
    // PENDING FTP
    // =========================================================

    public List<StationMessage> GetPendingFtpMessages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50)
    {
        using var connection = OpenConnection();

        var messages = GetPendingFtpMessagesInternal(
            connection,
            stationNames,
            ftp2: false,
            maxMessages);

        LoadMeasurements(
            connection,
            messages);

        return messages;
    }

    // =========================================================
    // PENDING FTP2
    // =========================================================

    public List<StationMessage> GetPendingFtp2Messages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50)
    {
        using var connection = OpenConnection();

        var messages = GetPendingFtpMessagesInternal(
            connection,
            stationNames,
            ftp2: true,
            maxMessages);

        LoadMeasurements(
            connection,
            messages);

        return messages;
    }

    // =========================================================
    // GET PENDING MESSAGES
    // =========================================================

    private static List<StationMessage> GetPendingMessages(
        SqliteConnection connection,
        string condition,
        int maxMessages)
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

        command.Parameters.AddWithValue(
            "$limit",
            maxMessages);

        return ReadMessages(command);
    }

    // =========================================================
    // GET PENDING FTP INTERNAL
    // =========================================================

    private static List<StationMessage>
        GetPendingFtpMessagesInternal(
            SqliteConnection connection,
            IEnumerable<string>? stationNames,
            bool ftp2,
            int maxMessages)
    {
        string sentColumn =
            ftp2 ? "ftp2_sent" : "ftp_sent";

        using var command = connection.CreateCommand();

        var names = stationNames?
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        string stationCondition = "";

        if (names is { Count: > 0 })
        {
            var parameters = new List<string>();

            for (int i = 0; i < names.Count; i++)
            {
                string parameterName = $"$station{i}";

                parameters.Add(parameterName);

                command.Parameters.AddWithValue(
                    parameterName,
                    names[i]);
            }

            stationCondition =
                $"AND station_name IN ({string.Join(", ", parameters)})";
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

        command.Parameters.AddWithValue(
            "$limit",
            maxMessages);

        return ReadMessages(command);
    }

    // =========================================================
    // READ MESSAGES
    // =========================================================

    private static List<StationMessage> ReadMessages(
        SqliteCommand command)
    {
        var messages = new List<StationMessage>();

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            messages.Add(new StationMessage
            {
                Id = reader.GetInt64(0),
                Timestamp = reader.GetString(1),
                StationName = reader.GetString(2),
                MqttSent = reader.GetInt64(3) != 0,
                FtpSent = reader.GetInt64(4) != 0,
                Ftp2Sent = reader.GetInt64(5) != 0
            });
        }

        return messages;
    }

    // =========================================================
    // LOAD MEASUREMENTS
    // =========================================================

    private static void LoadMeasurements(
        SqliteConnection connection,
        List<StationMessage> messages)
    {
        if (messages.Count == 0)
        {
            return;
        }

        var messageMap =
            messages.ToDictionary(x => x.Id);

        using var command = connection.CreateCommand();

        var parameters = new List<string>();

        for (int i = 0; i < messages.Count; i++)
        {
            string parameterName = $"$id{i}";

            parameters.Add(parameterName);

            command.Parameters.AddWithValue(
                parameterName,
                messages[i].Id);
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
            long messageId =
                reader.GetInt64(0);

            if (!messageMap.TryGetValue(
                    messageId,
                    out var message))
            {
                continue;
            }

            message.Measurements.Add(
                new SensorReading
                {
                    ParameterName =
                        reader.GetString(1),

                    Value =
                        reader.GetDouble(2),

                    Unit =
                        reader.GetString(3),

                    Status =
                        reader.GetInt32(4),

                    StationName =
                        reader.GetString(5),

                    Timestamp =
                        reader.GetString(6)
                });
        }
    }

    // =========================================================
    // MARK MQTT SENT
    // =========================================================

    public void MarkMqttSent(
        IEnumerable<long> messageIds)
    {
        MarkSent(
            messageIds,
            "mqtt_sent");
    }

    // =========================================================
    // MARK FTP SENT
    // =========================================================

    public void MarkFtpSent(
        IEnumerable<long> messageIds)
    {
        MarkSent(
            messageIds,
            "ftp_sent");
    }

    // =========================================================
    // MARK FTP2 SENT
    // =========================================================

    public void MarkFtp2Sent(
        IEnumerable<long> messageIds)
    {
        MarkSent(
            messageIds,
            "ftp2_sent");
    }

    // =========================================================
    // MARK SENT INTERNAL
    // =========================================================

    private void MarkSent(
        IEnumerable<long> messageIds,
        string columnName)
    {
        var ids = messageIds
            .Distinct()
            .ToList();

        if (ids.Count == 0)
        {
            return;
        }

        if (columnName != "mqtt_sent" &&
            columnName != "ftp_sent" &&
            columnName != "ftp2_sent")
        {
            throw new ArgumentException(
                "Invalid sent column.");
        }

        using var connection = OpenConnection();

        using var transaction =
            connection.BeginTransaction();

        try
        {
            foreach (long id in ids)
            {
                using var command =
                    connection.CreateCommand();

                command.Transaction =
                    transaction;

                command.CommandText = $"""
                    UPDATE station_messages
                    SET {columnName} = 1
                    WHERE message_id = $message_id;
                    """;

                command.Parameters.AddWithValue(
                    "$message_id",
                    id);

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
    // DELETE EXPIRED DATA
    // =========================================================

    private int DeleteExpiredData(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int retentionDays)
    {
        string cutoff =
            DateTime.Now
                .AddDays(-retentionDays)
                .ToString("yyyyMMddHHmmss");

        using var command =
            connection.CreateCommand();

        command.Transaction = transaction;

        command.CommandText = """
            DELETE FROM station_messages
            WHERE timestamp < $cutoff;
            """;

        command.Parameters.AddWithValue(
            "$cutoff",
            cutoff);

        return command.ExecuteNonQuery();
    }

    // =========================================================
    // DELETE OLDEST
    // =========================================================

    private int DeleteOldestMessages(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int batchSize)
    {
        using var command =
            connection.CreateCommand();

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

        command.Parameters.AddWithValue(
            "$batchSize",
            batchSize);

        return command.ExecuteNonQuery();
    }

    // =========================================================
    // CHECKPOINT WAL
    // =========================================================

    public void CheckpointWal()
    {
        using var connection = OpenConnection();

        using var command =
            connection.CreateCommand();

        command.CommandText =
            "PRAGMA wal_checkpoint(TRUNCATE);";

        command.ExecuteNonQuery();
    }

    // =========================================================
    // VACUUM
    // =========================================================

    public void Vacuum()
    {
        using var connection = OpenConnection();

        using var command =
            connection.CreateCommand();

        command.CommandText = "VACUUM;";

        command.ExecuteNonQuery();
    }

    // =========================================================
    // CLEANUP DATABASE
    // =========================================================

    public DatabaseCleanupResult CleanupDatabase(
        int retentionDays,
        int maxDatabaseSizeMB,
        bool vacuumAfterCleanup)
    {
        if (retentionDays <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retentionDays));
        }

        if (maxDatabaseSizeMB <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDatabaseSizeMB));
        }

        long maxBytes =
            (long)maxDatabaseSizeMB *
            1024 *
            1024;

        var result = new DatabaseCleanupResult
        {
            SizeBeforeBytes =
                GetDatabaseSizeBytes()
        };

        bool deleted = false;

        using (var connection = OpenConnection())
        using (var transaction =
               connection.BeginTransaction())
        {
            try
            {
                result.DeletedByRetention =
                    DeleteExpiredData(
                        connection,
                        transaction,
                        retentionDays);

                if (result.DeletedByRetention > 0)
                {
                    deleted = true;
                }

                while (GetDatabaseSizeBytes() > maxBytes)
                {
                    int count =
                        DeleteOldestMessages(
                            connection,
                            transaction,
                            1000);

                    if (count == 0)
                    {
                        break;
                    }

                    result.DeletedBySize += count;

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

        if (deleted)
        {
            CheckpointWal();

            if (vacuumAfterCleanup)
            {
                Vacuum();
            }
        }

        result.SizeAfterBytes =
            GetDatabaseSizeBytes();

        return result;
    }
}