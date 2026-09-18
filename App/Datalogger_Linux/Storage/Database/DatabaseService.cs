using Datalogger_Linux.Configuration;
using Datalogger_Linux.Models;
using Microsoft.Data.Sqlite;

namespace Datalogger_Linux.Services;

public class DatabaseService
{
    private readonly string _connectionString;
    private readonly string _databasePath;

    public DatabaseService()
    {
        // =====================================================
        // DATABASE PATH
        // =====================================================

        Directory.CreateDirectory(RuntimePaths.Database);

        _databasePath = RuntimePaths.DatabaseFile;

        _connectionString = $"Data Source={_databasePath}";

        Console.WriteLine($"SQLite Database: {_databasePath}");
    }

    // =========================================================
    // OPEN CONNECTION
    // =========================================================

    private SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(_connectionString);

        connection.Open();

        using var pragma = connection.CreateCommand();

        pragma.CommandText = """
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            """;

        pragma.ExecuteNonQuery();

        return connection;
    }

    // =========================================================
    // INITIALIZE DATABASE
    // =========================================================

    public void Initialize()
    {
        using var connection = OpenConnection();

        // -----------------------------------------------------
        // 1. CREATE TABLES
        // -----------------------------------------------------

        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                CREATE TABLE IF NOT EXISTS station_messages
                (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,

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

                    unit TEXT NOT NULL,

                    status INTEGER NOT NULL,

                    FOREIGN KEY (message_id)
                        REFERENCES station_messages(message_id)
                        ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS
                idx_station_messages_station_time
                ON station_messages
                (
                    station_name,
                    timestamp
                );

                CREATE INDEX IF NOT EXISTS
                idx_station_messages_timestamp
                ON station_messages
                (
                    timestamp,
                    message_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_station_messages_mqtt
                ON station_messages
                (
                    mqtt_sent,
                    message_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_station_messages_ftp
                ON station_messages
                (
                    ftp_sent,
                    message_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_measurements_message
                ON measurements
                (
                    message_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_measurements_name
                ON measurements
                (
                    name
                );
                """;

            command.ExecuteNonQuery();
        }

        // -----------------------------------------------------
        // 2. DATABASE MIGRATION
        //
        // Database cũ có thể chưa có ftp2_sent.
        // Phải migration TRƯỚC khi tạo index sử dụng ftp2_sent.
        // -----------------------------------------------------

        EnsureFtp2SentColumn(connection);

        // -----------------------------------------------------
        // 3. CREATE FTP2-DEPENDENT INDEXES
        // -----------------------------------------------------

        using (var indexCommand = connection.CreateCommand())
        {
            indexCommand.CommandText = """
                CREATE INDEX IF NOT EXISTS
                idx_station_messages_ftp2
                ON station_messages
                (
                    ftp2_sent,
                    message_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_station_messages_pending
                ON station_messages
                (
                    mqtt_sent,
                    ftp_sent,
                    ftp2_sent,
                    message_id
                );
                """;

            indexCommand.ExecuteNonQuery();
        }

        Console.WriteLine("SQLite database initialized.");
    }

    // =========================================================
    // ENSURE FTP2 COLUMN
    // =========================================================

    private static void EnsureFtp2SentColumn(SqliteConnection connection)
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

        reader.Close();

        if (exists)
            return;

        using var alterCommand = connection.CreateCommand();

        alterCommand.CommandText = """
            ALTER TABLE station_messages
            ADD COLUMN ftp2_sent INTEGER NOT NULL DEFAULT 0;
            """;

        alterCommand.ExecuteNonQuery();

        Console.WriteLine(
            "SQLite migration: added station_messages.ftp2_sent.");
    }

    // =========================================================
    // INSERT MANY STATION MESSAGES
    // =========================================================

    public void InsertStationMessages(
        IEnumerable<StationMessage> messages)
    {
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
                if (string.IsNullOrWhiteSpace(message.StationName))
                {
                    throw new ArgumentException(
                        "StationName không được rỗng.");
                }

                if (string.IsNullOrWhiteSpace(message.Timestamp))
                {
                    throw new ArgumentException(
                        "Timestamp không được rỗng.");
                }

                using var messageCommand = connection.CreateCommand();

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

                long messageId =
                    Convert.ToInt64(
                        messageCommand.ExecuteScalar());

                message.Id = messageId;

                foreach (SensorReading reading in message.Measurements)
                {
                    using var measurementCommand =
                        connection.CreateCommand();

                    measurementCommand.Transaction = transaction;

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
                        messageId);

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
            try
            {
                transaction.Rollback();
            }
            catch
            {
            }

            throw;
        }
    }

    // =========================================================
    // GET DATABASE SIZE
    // gateway.db
    // gateway.db-wal
    // gateway.db-shm
    // =========================================================

    public long GetDatabaseSizeBytes()
    {
        long totalSize = 0;

        string[] files =
        {
            _databasePath,
            _databasePath + "-wal",
            _databasePath + "-shm"
        };

        foreach (string file in files)
        {
            if (File.Exists(file))
            {
                totalSize += new FileInfo(file).Length;
            }
        }

        return totalSize;
    }

    // =========================================================
    // DELETE EXPIRED DATA
    // ON DELETE CASCADE
    // =========================================================

    private int DeleteExpiredData(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int retentionDays)
    {
        string cutoffTimestamp =
            DateTime.Now
                .AddDays(-retentionDays)
                .ToString("yyyyMMddHHmmss");

        using var command = connection.CreateCommand();

        command.Transaction = transaction;

        command.CommandText = """
            DELETE FROM station_messages
            WHERE timestamp < $cutoff;
            """;

        command.Parameters.AddWithValue(
            "$cutoff",
            cutoffTimestamp);

        return command.ExecuteNonQuery();
    }

    // =========================================================
    // DELETE OLDEST MESSAGES
    // DÙNG KHI DATABASE VƯỢT MAX SIZE.
    // =========================================================

    private int DeleteOldestMessages(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize));
        }

        using var connection = OpenConnection();

        using var transaction = connection.BeginTransaction();

        try
        {
            using var command = connection.CreateCommand();

            command.Transaction = transaction;

            command.CommandText = """
                DELETE FROM station_messages
                WHERE message_id IN
                (
                    SELECT message_id
                    FROM station_messages
                    ORDER BY
                        timestamp ASC,
                        message_id ASC
                    LIMIT $limit
                );
                """;

            command.Parameters.AddWithValue(
                "$limit",
                batchSize);

            int deletedRows =
                command.ExecuteNonQuery();

            transaction.Commit();

            return deletedRows;
        }
        catch
        {
            try
            {
                transaction.Rollback();
            }
            catch
            {
            }

            throw;
        }
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
    // CHECKPOINT WAL
    // =========================================================

    private void CheckpointWal()
    {
        using var connection = OpenConnection();

        using var command = connection.CreateCommand();

        command.CommandText =
            "PRAGMA wal_checkpoint(TRUNCATE);";

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
                nameof(retentionDays),
                "RetentionDays phải lớn hơn 0.");
        }

        if (maxDatabaseSizeMB <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDatabaseSizeMB),
                "MaxDatabaseSizeMB phải lớn hơn 0.");
        }

        long maxSizeBytes =
            maxDatabaseSizeMB * 1024L * 1024L;

        long sizeBefore =
            GetDatabaseSizeBytes();

        int deletedByRetention = 0;
        int deletedBySize = 0;

        // 1. DELETE BY RETENTION

        using (var connection = OpenConnection())
        {
            using var transaction =
                connection.BeginTransaction();

            try
            {
                deletedByRetention =
                    DeleteExpiredData(
                        connection,
                        transaction,
                        retentionDays);

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
                }

                throw;
            }
        }

        // 2. CHECK DATABASE SIZE

        long currentSize =
            GetDatabaseSizeBytes();

        // 3. DELETE BY SIZE

        while (currentSize > maxSizeBytes)
        {
            int deleted =
                DeleteOldestMessages(1000);

            if (deleted == 0)
            {
                break;
            }

            deletedBySize += deleted;

            currentSize =
                GetDatabaseSizeBytes();
        }

        // 4. CHECKPOINT WAL

        if (deletedByRetention > 0 ||
            deletedBySize > 0)
        {
            CheckpointWal();
        }

        // 5. VACUUM

        if (vacuumAfterCleanup &&
            (deletedByRetention > 0 ||
             deletedBySize > 0))
        {
            Vacuum();
        }

        // 6. FINAL SIZE

        long sizeAfter =
            GetDatabaseSizeBytes();

        return new DatabaseCleanupResult
        {
            SizeBeforeBytes = sizeBefore,

            SizeAfterBytes = sizeAfter,

            DeletedByRetention = deletedByRetention,

            DeletedBySize = deletedBySize,
        };
    }

    // =========================================================
    // FTP1
    // GET PENDING FTP MESSAGES
    // =========================================================

    public List<StationMessage> GetPendingFtpMessages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50)
    {
        return GetPendingFtpMessagesInternal(
            ftp2: false,
            stationNames,
            maxMessages);
    }

    // =========================================================
    // FTP2
    // GET PENDING FTP2 MESSAGES
    // =========================================================

    public List<StationMessage> GetPendingFtp2Messages(
        IEnumerable<string>? stationNames = null,
        int maxMessages = 50)
    {
        return GetPendingFtpMessagesInternal(
            ftp2: true,
            stationNames,
            maxMessages);
    }

    // =========================================================
    // GET PENDING FTP MESSAGES - INTERNAL
    // =========================================================

    private List<StationMessage> GetPendingFtpMessagesInternal(
        bool ftp2,
        IEnumerable<string>? stationNames,
        int maxMessages)
    {
        if (maxMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxMessages));
        }

        List<string>? stationList = stationNames
            ?.Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (stationList is not null &&
            stationList.Count == 0)
        {
            return new List<StationMessage>();
        }

        List<StationMessage> messages = new();

        using var connection = OpenConnection();

        using var command = connection.CreateCommand();

        string sentColumn =
            ftp2 ? "ftp2_sent" : "ftp_sent";

        if (stationList is null)
        {
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
                ORDER BY
                    timestamp ASC,
                    message_id ASC
                LIMIT $limit;
                """;

            command.Parameters.AddWithValue(
                "$limit",
                maxMessages);
        }
        else
        {
            List<string> parameterNames = new();

            for (int i = 0; i < stationList.Count; i++)
            {
                string parameterName =
                    $"$station{i}";

                parameterNames.Add(parameterName);

                command.Parameters.AddWithValue(
                    parameterName,
                    stationList[i]);
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
                  AND station_name IN
                  (
                      {string.Join(
                          ", ",
                          parameterNames
                      )}
                  )
                ORDER BY
                    timestamp ASC,
                    message_id ASC
                LIMIT $limit;
                """;

            command.Parameters.AddWithValue(
                "$limit",
                maxMessages);
        }

        using var reader =
            command.ExecuteReader();

        while (reader.Read())
        {
            messages.Add(
                new StationMessage
                {
                    Id = reader.GetInt64(0),

                    Timestamp = reader.GetString(1),

                    StationName = reader.GetString(2),

                    MqttSent =
                        reader.GetInt32(3) != 0,

                    FtpSent =
                        reader.GetInt32(4) != 0,

                    Ftp2Sent =
                        reader.GetInt32(5) != 0,

                    Measurements =
                        new List<SensorReading>(),
                });
        }

        reader.Close();

        foreach (StationMessage message in messages)
        {
            using var measurementCommand =
                connection.CreateCommand();

            measurementCommand.CommandText = """
                SELECT
                    name,
                    value,
                    unit,
                    status
                FROM measurements
                WHERE message_id = $message_id
                ORDER BY rowid ASC;
                """;

            measurementCommand.Parameters.AddWithValue(
                "$message_id",
                message.Id);

            using var measurementReader =
                measurementCommand.ExecuteReader();

            while (measurementReader.Read())
            {
                message.Measurements.Add(
                    new SensorReading
                    {
                        StationName =
                            message.StationName,

                        ParameterName =
                            measurementReader.GetString(0),

                        Value =
                            measurementReader.GetDouble(1),

                        Unit =
                            measurementReader.GetString(2),

                        Status =
                            measurementReader.GetInt32(3),

                        Timestamp =
                            message.Timestamp,
                    });
            }
        }

        return messages;
    }

    // =========================================================
    // FTP1 - MARK SENT
    // =========================================================

    public void MarkFtpSent(
        IEnumerable<long> messageIds)
    {
        MarkFtpSentInternal(
            messageIds,
            ftp2: false);
    }

    // =========================================================
    // FTP2 - MARK SENT
    // =========================================================

    public void MarkFtp2Sent(
        IEnumerable<long> messageIds)
    {
        MarkFtpSentInternal(
            messageIds,
            ftp2: true);
    }

    // =========================================================
    // MARK FTP SENT - INTERNAL
    // =========================================================

    private void MarkFtpSentInternal(
        IEnumerable<long> messageIds,
        bool ftp2)
    {
        List<long> ids =
            messageIds
                .Where(id => id > 0)
                .Distinct()
                .ToList();

        if (ids.Count == 0)
            return;

        using var connection = OpenConnection();

        using var transaction =
            connection.BeginTransaction();

        try
        {
            using var command =
                connection.CreateCommand();

            command.Transaction = transaction;

            string sentColumn =
                ftp2 ? "ftp2_sent" : "ftp_sent";

            command.CommandText = $"""
                UPDATE station_messages
                SET {sentColumn} = 1
                WHERE message_id = $message_id;
                """;

            var parameter =
                command.Parameters.Add(
                    "$message_id",
                    SqliteType.Integer);

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
            }

            throw;
        }
    }

    // =========================================================
    // MQTT - GET PENDING
    // =========================================================

    public List<StationMessage> GetPendingMqttMessages(
        int maxMessages = 5)
    {
        if (maxMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxMessages));
        }

        List<StationMessage> messages = new();

        using var connection = OpenConnection();

        using var command =
            connection.CreateCommand();

        command.CommandText = """
            SELECT
                message_id,
                timestamp,
                station_name,
                mqtt_sent,
                ftp_sent,
                ftp2_sent
            FROM station_messages
            WHERE mqtt_sent = 0
            ORDER BY
                timestamp ASC,
                message_id ASC
            LIMIT @limit;
            """;

        command.Parameters.AddWithValue(
            "@limit",
            maxMessages);

        using var reader =
            command.ExecuteReader();

        while (reader.Read())
        {
            var message = new StationMessage
            {
                Id = reader.GetInt64(0),

                Timestamp = reader.GetString(1),

                StationName = reader.GetString(2),

                MqttSent =
                    reader.GetInt32(3) != 0,

                FtpSent =
                    reader.GetInt32(4) != 0,

                Ftp2Sent =
                    reader.GetInt32(5) != 0,

                Measurements =
                    new List<SensorReading>(),
            };

            messages.Add(message);
        }

        reader.Close();

        foreach (var message in messages)
        {
            using var measurementCommand =
                connection.CreateCommand();

            measurementCommand.CommandText = """
                SELECT
                    name,
                    value,
                    unit,
                    status
                FROM measurements
                WHERE message_id = @id
                ORDER BY rowid;
                """;

            measurementCommand.Parameters.AddWithValue(
                "@id",
                message.Id);

            using var measurementReader =
                measurementCommand.ExecuteReader();

            while (measurementReader.Read())
            {
                message.Measurements.Add(
                    new SensorReading
                    {
                        StationName =
                            message.StationName,

                        ParameterName =
                            measurementReader.GetString(0),

                        Value =
                            measurementReader.GetDouble(1),

                        Unit =
                            measurementReader.GetString(2),

                        Status =
                            measurementReader.GetInt32(3),

                        Timestamp =
                            message.Timestamp,
                    });
            }
        }

        return messages;
    }

    // =========================================================
    // MQTT - MARK SENT
    // =========================================================

    public void MarkMqttSent(
        IEnumerable<long> messageIds)
    {
        List<long> ids =
            messageIds
                .Where(id => id > 0)
                .Distinct()
                .ToList();

        if (ids.Count == 0)
            return;

        using var connection = OpenConnection();

        using var transaction =
            connection.BeginTransaction();

        try
        {
            using var command =
                connection.CreateCommand();

            command.Transaction = transaction;

            command.CommandText = """
                UPDATE station_messages
                SET mqtt_sent = 1
                WHERE message_id = @id;
                """;

            var parameter =
                command.Parameters.Add(
                    "@id",
                    SqliteType.Integer);

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
            }

            throw;
        }
    }
}

// =========================================================
// CLEANUP RESULT
// =========================================================

public class DatabaseCleanupResult
{
    public long SizeBeforeBytes { get; set; }

    public long SizeAfterBytes { get; set; }

    public int DeletedByRetention { get; set; }

    public int DeletedBySize { get; set; }

    public int TotalDeleted =>
        DeletedByRetention + DeletedBySize;
}