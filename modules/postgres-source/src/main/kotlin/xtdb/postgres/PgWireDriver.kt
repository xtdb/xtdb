package xtdb.postgres

import kotlinx.coroutines.delay
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.core.transaction.TransactionIsolationLevel.REPEATABLE_READ
import org.postgresql.PGConnection
import org.postgresql.PGProperty
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.replication.PGReplicationStream
import org.postgresql.util.PSQLException
import xtdb.util.debug
import xtdb.util.info
import xtdb.util.logger
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

private val LOG = PgWireDriver::class.logger

private const val SNAPSHOT_BATCH_SIZE = 1000

private const val SLOT_RETRY_MAX_ATTEMPTS = 7
private const val SLOT_RETRY_BASE_DELAY_MS = 1000L
private val SLOT_ACTIVE_PATTERN = Regex(".*replication slot .* is active.*")

class PgWireDriver(
    private val dbName: String,
    private val hostname: String,
    private val port: Int,
    private val database: String,
    private val username: String,
    private val password: String,
    private val slotName: String,
    private val publicationName: String,
    private val schemaIncludeList: List<String>,
) : PostgresDriver {

    private data class ColumnInfo(val name: String, val typeOid: Int)

    // PGProperty.set() is the correct API here — it resolves the internal property name string.
    // props[PGProperty.USER] = "..." silently breaks: it puts the enum object as the key,
    // but pgjdbc looks up by string (e.g. "user"), so the password is never found.
    fun openReplicationConnection(): Connection =
        DriverManager.getConnection(
            "jdbc:postgresql://$hostname:$port/$database",
            Properties().also {
                PGProperty.USER.set(it, username)
                PGProperty.PASSWORD.set(it, password)
                PGProperty.ASSUME_MIN_SERVER_VERSION.set(it, "15")
                PGProperty.REPLICATION.set(it, "database")
                PGProperty.PREFER_QUERY_MODE.set(it, "simple")
            })

    val jdbi: Jdbi by lazy {
        Jdbi.create("jdbc:postgresql://$hostname:$port/$database", username, password)
            .installPlugin(KotlinPlugin())
    }

    // --- Snapshot ---

    override fun openSnapshot(): PostgresDriver.SnapshotReader {
        LOG.debug { "[$dbName] Opening replication connection to $hostname:$port/$database" }

        val replConn = openReplicationConnection()
        try {
            val pgReplConn = replConn.unwrap(PGConnection::class.java)

            LOG.debug { "[$dbName] Creating replication slot '$slotName' with pgoutput" }

            val slotInfo = pgReplConn.replicationAPI
                .createReplicationSlot()
                .logical()
                .withSlotName(slotName)
                .withOutputPlugin("pgoutput")
                .make()

            val snapshotName = slotInfo.snapshotName!!
            val slotLsn = slotInfo.consistentPoint.asLong()

            LOG.info("[$dbName] Created slot '$slotName' at LSN ${LogSequenceNumber.valueOf(slotLsn)}, snapshot=$snapshotName")

            return PgWireSnapshotReader(replConn, snapshotName, slotLsn)
        } catch (e: Throwable) {
            replConn.close()
            throw e
        }
    }

    private inner class PgWireSnapshotReader(
        private val replConn: Connection,
        private val snapshotName: String,
        override val slotLsn: Long,
    ) : PostgresDriver.SnapshotReader {

        override fun batches(): Sequence<List<RowOp.Put>> = sequence {
            jdbi.open().use { handle ->
                handle.begin()
                try {
                    handle.transactionIsolationLevel = REPEATABLE_READ

                    LOG.debug { "[$dbName] SET TRANSACTION SNAPSHOT '$snapshotName'" }
                    handle.execute("SET TRANSACTION SNAPSHOT '$snapshotName'")

                    yieldAll(readSnapshotBatches(handle))
                } finally {
                    handle.rollback()
                }
            }
        }

        override fun close() {
            replConn.close()
        }
    }

    private fun readSnapshotBatches(handle: Handle): Sequence<List<RowOp.Put>> = sequence {
        val tables = handle.discoverTables()
        LOG.info("[$dbName] Discovered ${tables.size} tables in publication '$publicationName': ${tables.joinToString { "${it.first}.${it.second}" }}")

        for ((schema, table) in tables) {
            val columns = handle.discoverColumns(schema, table)
            val fullTableName = "$schema.$table"

            LOG.info("[$dbName] Snapshotting $fullTableName (${columns.size} columns: ${columns.joinToString { it.name }})")

            var rowCount = 0
            handle.createQuery("SELECT * FROM \"$schema\".\"$table\"")
                .setFetchSize(SNAPSHOT_BATCH_SIZE)
                .map { rs, _ -> columns.associate { col -> col.name to rs.getObject(col.name) } }
                .iterator().use { iter ->
                    iter.asSequence()
                        .chunked(SNAPSHOT_BATCH_SIZE)
                        .forEach { batch ->
                            rowCount += batch.size
                            LOG.debug { "[$dbName] Flushing $fullTableName batch: ${batch.size} rows (total: $rowCount)" }
                            yield(batch.map { row -> RowOp.Put(schema, table, row) })
                        }
                }

            LOG.info("[$dbName] Finished snapshotting $fullTableName ($rowCount rows)")
        }
    }

    // --- Table discovery (used internally by snapshot) ---

    private fun Handle.discoverTables(): List<Pair<String, String>> =
        createQuery(
            """
            SELECT schemaname, tablename FROM pg_publication_tables
            WHERE pubname = :pubName AND schemaname IN (<schemas>)
            ORDER BY schemaname, tablename""".trimIndent()
        )
            .bind("pubName", publicationName)
            .bindList("schemas", schemaIncludeList)
            .map { rs, _ -> rs.getString("schemaname") to rs.getString("tablename") }
            .list()

    private fun Handle.discoverColumns(schema: String, table: String): List<ColumnInfo> =
        createQuery(
            """
            SELECT a.attname, a.atttypid::int
            FROM pg_attribute a
              JOIN pg_class c ON a.attrelid = c.oid
              JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = :schema AND c.relname = :table
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum""".trimIndent()
        )
            .bind("schema", schema)
            .bind("table", table)
            .map { rs, _ -> ColumnInfo(rs.getString("attname"), rs.getInt("atttypid")) }
            .list()

    // --- Streaming ---

    /**
     * Retries starting the replication stream when the slot is still held by a previous connection
     * (e.g. after leadership handover). PG's wal_sender_timeout (default 60s) will kill the old
     * connection eventually — we just need to wait it out.
     */
    suspend fun startReplicationStream(pgReplConn: PGConnection, startLsn: Long): PGReplicationStream {
        for (attempt in 1..SLOT_RETRY_MAX_ATTEMPTS) {
            try {
                return pgReplConn.replicationAPI
                    .replicationStream()
                    .logical()
                    .withSlotName(slotName)
                    .withStartPosition(LogSequenceNumber.valueOf(startLsn))
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", publicationName)
                    .start()
            } catch (e: PSQLException) {
                if (!SLOT_ACTIVE_PATTERN.matches(e.message ?: "")) throw e
                if (attempt == SLOT_RETRY_MAX_ATTEMPTS) throw e

                val baseDelay = SLOT_RETRY_BASE_DELAY_MS shl (attempt - 1)
                val delayMs = baseDelay + (baseDelay * 0.5 * Math.random()).toLong()

                LOG.info("[$dbName] Replication slot '$slotName' is active (attempt $attempt/$SLOT_RETRY_MAX_ATTEMPTS), retrying in ${delayMs}ms")
                delay(delayMs)
            }
        }

        error("unreachable")
    }

    override fun close() {}
}
