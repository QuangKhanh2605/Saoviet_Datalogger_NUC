using System.Globalization;
using Database.Configuration;
using Database.Models;
using Microsoft.Data.Sqlite;

namespace Database.Services;

public sealed class DatabaseService
{
    private readonly string _databasePath;
    private readonly string _connectionString;
    private readonly object _initializationLock = new();

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
    // INITIALIZATION
    // =========================================================

    public void Initialize()
    {
        lock (_initializationLock)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();

            command.CommandText = """
                CREATE TABLE IF NOT EXISTS station_messages
                (
                    message_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    station_name TEXT NOT NULL,
                    mqtt_sent INTEGER NOT NULL DEFAULT 0,
                    ftp_sent INTEGER NOT NULL DEFAULT 0,
                    ftp2_sent INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS measurements
                (
                    message_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL DEFAULT '',
                    status INTEGER NOT NULL DEFAULT 0,
                    slave_id INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(message_id)
                        REFERENCES station_messages(message_id)
                        ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_station_messages_station_timestamp
                    ON station_messages(station_name, timestamp);

                CREATE INDEX IF NOT EXISTS idx_station_messages_timestamp
                    ON station_messages(timestamp);

                CREATE INDEX IF NOT EXISTS idx_station_messages_mqtt_sent
                    ON station_messages(mqtt_sent, timestamp, message_id);

                CREATE INDEX IF NOT EXISTS idx_station_messages_ftp_sent
                    ON station_messages(ftp_sent, timestamp, message_id);

                CREATE INDEX IF NOT EXISTS idx_station_messages_ftp2_sent
                    ON station_messages(ftp2_sent, timestamp, message_id);

                CREATE INDEX IF NOT EXISTS idx_measurements_message_id
                    ON measurements(message_id);
                """;

            command.ExecuteNonQuery();

            EnsureColumnExists(
                connection,
                "measurements",
                "slave_id",
                "INTEGER NOT NULL DEFAULT 0"
            );

            Console.WriteLine("SQLite database initialized.");
        }
    }

    private static void EnsureColumnExists(
        SqliteConnection connection,
        string tableName,
        string columnName,
        string columnDefinition
    )
    {
        using var command = connection.CreateCommand();

        command.CommandText = $"PRAGMA table_info({tableName});";

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            if (
                string.Equals(
                    reader["name"]?.ToString(),
                    columnName,
                    StringComparison.OrdinalIgnoreCase
                )
            )
            {
                return;
            }
        }

        reader.Close();

        using var alterCommand = connection.CreateCommand();

        alterCommand.CommandText =
            $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnDefinition};";

        alterCommand.ExecuteNonQuery();
    }

    // =========================================================
    // SAVE MESSAGES
    // =========================================================

    public void SaveMqttMessages(IEnumerable<StationMessage> messages)
    {
        SaveQueueMessages(messages, "mqtt_sent");
    }

    public void SaveFtpMessages(IEnumerable<StationMessage> messages)
    {
        SaveQueueMessages(messages, "ftp_sent");
    }

    public void SaveFtp2Messages(IEnumerable<StationMessage> messages)
    {
        SaveQueueMessages(messages, "ftp2_sent");
    }

    private void SaveQueueMessages(IEnumerable<StationMessage> messages, string sentColumn)
    {
        ValidateSentColumn(sentColumn);

        List<StationMessage> messageList = messages.ToList();

        if (messageList.Count == 0)
        {
            return;
        }

        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            foreach (StationMessage message in messageList)
            {
                ValidateMessage(message);

                bool exists = MessageExists(connection, transaction, message.Id);

                if (!exists)
                {
                    InsertStationMessage(connection, transaction, message);

                    InsertMeasurements(connection, transaction, message);
                }

                bool sent = sentColumn switch
                {
                    "mqtt_sent" => message.MqttSent,
                    "ftp_sent" => message.FtpSent,
                    "ftp2_sent" => message.Ftp2Sent,
                    _ => false,
                };

                if (sent)
                {
                    using var updateCommand = connection.CreateCommand();

                    updateCommand.Transaction = transaction;
                    updateCommand.CommandText = $"""
                        UPDATE station_messages
                        SET {sentColumn} = 1
                        WHERE message_id = @messageId;
                        """;

                    updateCommand.Parameters.AddWithValue("@messageId", message.Id);

                    updateCommand.ExecuteNonQuery();
                }
            }

            transaction.Commit();
        }
        catch
        {
            try
            {
                transaction.Rollback();
            }
            catch
            {
                // Preserve the original exception.
            }

            throw;
        }
    }

    private static void ValidateMessage(StationMessage message)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (message.Id <= 0)
        {
            throw new ArgumentException("Message Id must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(message.Timestamp))
        {
            throw new ArgumentException($"Timestamp is empty for message {message.Id}.");
        }

        if (string.IsNullOrWhiteSpace(message.StationName))
        {
            throw new ArgumentException($"StationName is empty for message {message.Id}.");
        }
    }

    private static bool MessageExists(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long messageId
    )
    {
        using var command = connection.CreateCommand();

        command.Transaction = transaction;
        command.CommandText = """
            SELECT EXISTS
            (
                SELECT 1
                FROM station_messages
                WHERE message_id = @messageId
            );
            """;

        command.Parameters.AddWithValue("@messageId", messageId);

        return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture) == 1;
    }

    // =========================================================
    // PUBLIC MESSAGE EXISTS
    // =========================================================

    /// <summary>
    /// Kiểm tra message có tồn tại trong SQLite hay không.
    /// Nếu SQLite gặp lỗi, exception sẽ được ném ra để tránh
    /// xóa nhầm Redis status key.
    /// </summary>
    public bool MessageExists(long messageId)
    {
        if (messageId <= 0)
        {
            return false;
        }

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
            SELECT EXISTS
            (
                SELECT 1
                FROM station_messages
                WHERE message_id = @messageId
            );
            """;

        command.Parameters.AddWithValue("@messageId", messageId);

        object? result = command.ExecuteScalar();

        return Convert.ToInt32(result, CultureInfo.InvariantCulture) == 1;
    }

    private static void InsertStationMessage(
        SqliteConnection connection,
        SqliteTransaction transaction,
        StationMessage message
    )
    {
        using var command = connection.CreateCommand();

        command.Transaction = transaction;
        command.CommandText = """
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
                @messageId,
                @timestamp,
                @stationName,
                @mqttSent,
                @ftpSent,
                @ftp2Sent
            );
            """;

        command.Parameters.AddWithValue("@messageId", message.Id);
        command.Parameters.AddWithValue("@timestamp", message.Timestamp);
        command.Parameters.AddWithValue("@stationName", message.StationName);
        command.Parameters.AddWithValue("@mqttSent", message.MqttSent ? 1 : 0);
        command.Parameters.AddWithValue("@ftpSent", message.FtpSent ? 1 : 0);
        command.Parameters.AddWithValue("@ftp2Sent", message.Ftp2Sent ? 1 : 0);

        command.ExecuteNonQuery();
    }

    private static void InsertMeasurements(
        SqliteConnection connection,
        SqliteTransaction transaction,
        StationMessage message
    )
    {
        if (message.Measurements is null || message.Measurements.Count == 0)
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
                status,
                slave_id
            )
            VALUES
            (
                @messageId,
                @name,
                @value,
                @unit,
                @status,
                @slaveId
            );
            """;

        var messageIdParameter = command.Parameters.Add("@messageId", SqliteType.Integer);

        var nameParameter = command.Parameters.Add("@name", SqliteType.Text);

        var valueParameter = command.Parameters.Add("@value", SqliteType.Real);

        var unitParameter = command.Parameters.Add("@unit", SqliteType.Text);

        var statusParameter = command.Parameters.Add("@status", SqliteType.Integer);

        var slaveIdParameter = command.Parameters.Add("@slaveId", SqliteType.Integer);

        foreach (SensorReading reading in message.Measurements)
        {
            messageIdParameter.Value = message.Id;
            nameParameter.Value = reading.Name ?? "";
            valueParameter.Value = reading.Value;
            unitParameter.Value = reading.Unit ?? "";
            statusParameter.Value = reading.Status;
            slaveIdParameter.Value = reading.SlaveId;

            command.ExecuteNonQuery();
        }
    }

    // =========================================================
    // PENDING MESSAGES
    // =========================================================

    public List<StationMessage> GetPendingMqttMessages(int maxMessages = 5)
    {
        return GetPendingMessages("mqtt_sent", maxMessages);
    }

    public List<StationMessage> GetPendingFtpMessages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50
    )
    {
        return GetPendingFtpMessagesInternal("ftp_sent", stationNames, maxMessages);
    }

    public List<StationMessage> GetPendingFtp2Messages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50
    )
    {
        return GetPendingFtpMessagesInternal("ftp2_sent", stationNames, maxMessages);
    }

    private List<StationMessage> GetPendingMessages(string sentColumn, int maxMessages)
    {
        ValidateSentColumn(sentColumn);

        if (maxMessages <= 0)
        {
            return new List<StationMessage>();
        }

        using var connection = OpenConnection();
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
            WHERE {sentColumn} = 0
            ORDER BY timestamp, message_id
            LIMIT @limit;
            """;

        command.Parameters.AddWithValue("@limit", maxMessages);

        List<StationMessage> messages = ReadMessages(connection, command);

        LoadMeasurements(connection, messages);

        return messages;
    }

    private List<StationMessage> GetPendingFtpMessagesInternal(
        string sentColumn,
        IEnumerable<string>? stationNames,
        int maxMessages
    )
    {
        ValidateSentColumn(sentColumn);

        if (maxMessages <= 0)
        {
            return new List<StationMessage>();
        }

        string[] stations =
            stationNames
                ?.Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray()
            ?? Array.Empty<string>();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        string stationFilter = "";

        if (stations.Length > 0)
        {
            string[] parameterNames = new string[stations.Length];

            for (int i = 0; i < stations.Length; i++)
            {
                parameterNames[i] = $"@station{i}";

                command.Parameters.AddWithValue(parameterNames[i], stations[i]);
            }

            stationFilter = $" AND station_name IN ({string.Join(", ", parameterNames)})";
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
            {stationFilter}
            ORDER BY timestamp, message_id
            LIMIT @limit;
            """;

        command.Parameters.AddWithValue("@limit", maxMessages);

        List<StationMessage> messages = ReadMessages(connection, command);

        LoadMeasurements(connection, messages);

        return messages;
    }

    private static void ValidateSentColumn(string sentColumn)
    {
        if (sentColumn is not ("mqtt_sent" or "ftp_sent" or "ftp2_sent"))
        {
            throw new ArgumentException("Invalid sent column.", nameof(sentColumn));
        }
    }

    private static List<StationMessage> ReadMessages(
        SqliteConnection connection,
        SqliteCommand command
    )
    {
        List<StationMessage> messages = new();

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            messages.Add(
                new StationMessage
                {
                    Id = reader.GetInt64(0),
                    Timestamp = reader.GetString(1),
                    StationName = reader.GetString(2),
                    MqttSent = reader.GetInt32(3) == 1,
                    FtpSent = reader.GetInt32(4) == 1,
                    Ftp2Sent = reader.GetInt32(5) == 1,
                }
            );
        }

        return messages;
    }

    private static void LoadMeasurements(SqliteConnection connection, List<StationMessage> messages)
    {
        if (messages.Count == 0)
        {
            return;
        }

        Dictionary<long, StationMessage> messageById = messages.ToDictionary(message => message.Id);

        using var command = connection.CreateCommand();

        string[] parameters = new string[messages.Count];

        for (int i = 0; i < messages.Count; i++)
        {
            parameters[i] = $"@id{i}";

            command.Parameters.AddWithValue(parameters[i], messages[i].Id);
        }

        command.CommandText = $"""
            SELECT
                message_id,
                name,
                value,
                unit,
                status,
                slave_id
            FROM measurements
            WHERE message_id IN ({string.Join(", ", parameters)})
            ORDER BY message_id;
            """;

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            long messageId = reader.GetInt64(0);

            if (!messageById.TryGetValue(messageId, out StationMessage? message))
            {
                continue;
            }

            message.Measurements.Add(
                new SensorReading
                {
                    StationName = message.StationName,
                    Timestamp = message.Timestamp,
                    Name = reader.GetString(1),
                    Value = reader.GetDouble(2),
                    Unit = reader.GetString(3),
                    Status = reader.GetInt32(4),
                    SlaveId = checked((byte)reader.GetInt32(5)),
                }
            );
        }
    }

    // =========================================================
    // MARK SENT
    // =========================================================

    public void MarkMqttSent(IEnumerable<long> messageIds)
    {
        MarkSent(messageIds, "mqtt_sent");
    }

    public void MarkFtpSent(IEnumerable<long> messageIds)
    {
        MarkSent(messageIds, "ftp_sent");
    }

    public void MarkFtp2Sent(IEnumerable<long> messageIds)
    {
        MarkSent(messageIds, "ftp2_sent");
    }

    private void MarkSent(IEnumerable<long> messageIds, string columnName)
    {
        ValidateSentColumn(columnName);

        long[] ids = messageIds.Where(id => id > 0).Distinct().ToArray();

        if (ids.Length == 0)
        {
            return;
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
                WHERE message_id = @messageId;
                """;

            var parameter = command.Parameters.Add("@messageId", SqliteType.Integer);

            foreach (long id in ids)
            {
                parameter.Value = id;
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch
        {
            try
            {
                transaction.Rollback();
            }
            catch
            {
                // Preserve the original exception.
            }

            throw;
        }
    }

    // =========================================================
    // CLEANUP
    // =========================================================

    public long GetDatabaseSizeBytes()
    {
        long size = 0;

        string[] files = { _databasePath, _databasePath + "-wal", _databasePath + "-shm" };

        foreach (string file in files)
        {
            if (File.Exists(file))
            {
                size += new FileInfo(file).Length;
            }
        }

        return size;
    }

    public int DeleteExpiredData(int retentionDays)
    {
        if (retentionDays <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionDays));
        }

        string cutoff = DateTime
            .Now.AddDays(-retentionDays)
            .ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
            DELETE FROM station_messages
            WHERE timestamp < @cutoff;
            """;

        command.Parameters.AddWithValue("@cutoff", cutoff);

        return command.ExecuteNonQuery();
    }

    public int DeleteOldestMessages(int batchSize)
    {
        if (batchSize <= 0)
        {
            return 0;
        }

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = """
            DELETE FROM station_messages
            WHERE message_id IN
            (
                SELECT message_id
                FROM station_messages
                ORDER BY timestamp, message_id
                LIMIT @limit
            );
            """;

        command.Parameters.AddWithValue("@limit", batchSize);

        return command.ExecuteNonQuery();
    }

    public void CheckpointWal()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
        command.ExecuteNonQuery();
    }

    public void Vacuum()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();

        command.CommandText = "VACUUM;";
        command.ExecuteNonQuery();
    }

    public void CleanupDatabase(int retentionDays, long maxDatabaseSizeMB, bool vacuumAfterCleanup)
    {
        if (retentionDays <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionDays));
        }

        if (maxDatabaseSizeMB <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDatabaseSizeMB));
        }

        int deletedExpired = DeleteExpiredData(retentionDays);

        long maxBytes = checked(maxDatabaseSizeMB * 1024L * 1024L);

        const int batchSize = 1000;

        while (GetDatabaseSizeBytes() > maxBytes)
        {
            int deleted = DeleteOldestMessages(batchSize);

            if (deleted == 0)
            {
                break;
            }
        }

        CheckpointWal();

        if (vacuumAfterCleanup)
        {
            Vacuum();
        }

        Console.WriteLine(
            $"Database cleanup completed. Expired messages deleted: {deletedExpired}. Current size: {GetDatabaseSizeBytes()} bytes."
        );
    }
}
