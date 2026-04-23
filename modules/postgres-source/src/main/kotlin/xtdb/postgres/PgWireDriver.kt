package xtdb.postgres

import kotlinx.coroutines.delay
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.postgresql.PGConnection
import org.postgresql.PGProperty
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.replication.PGReplicationStream
import org.postgresql.util.PSQLException
import xtdb.util.info
import xtdb.util.logger
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

private val LOG = PgWireDriver::class.logger

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
) : AutoCloseable {

    data class ColumnInfo(val name: String, val typeOid: Int)

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

    fun Handle.discoverTables(): List<Pair<String, String>> =
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

    fun Handle.discoverColumns(schema: String, table: String): List<ColumnInfo> =
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
