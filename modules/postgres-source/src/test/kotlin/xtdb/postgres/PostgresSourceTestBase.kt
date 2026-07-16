package xtdb.postgres

import kotlinx.coroutines.runInterruptible
import org.junit.jupiter.api.fail
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.log.Log.Companion.localLog
import xtdb.api.storage.Storage
import xtdb.postgres.proto.PostgresSourceToken
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Shared plumbing for the Postgres-source CDC tests: a logical-replication Postgres container plus
 * helpers to drive it (pg writes), stand up a local-log/storage XTDB node with the cdc db attached,
 * query the cdc db over pgwire, and await streaming / block flushes.
 *
 * The container is a singleton — started once on first use and reaped by Ryuk at JVM exit — so the
 * whole module's CDC tests share one instance rather than each paying a start/stop.
 */
abstract class PostgresSourceTestBase {

    // --- pg ---

    protected fun pgConn(pg: PostgreSQLContainer = postgres): Connection =
        DriverManager.getConnection(pg.jdbcUrl, pg.username, pg.password)

    protected fun pgExecute(pg: PostgreSQLContainer, vararg statements: String) =
        pgConn(pg).use { c -> c.createStatement().use { s -> statements.forEach { s.execute(it) } } }

    protected fun pgExecute(vararg statements: String) = pgExecute(postgres, *statements)

    protected fun unique(prefix: String) = "${prefix}_${UUID.randomUUID().toString().replace("-", "_")}"

    /** Runs a single prepared write in its own connection wrapped in an explicit BEGIN/COMMIT, so
     * it lands as its own transaction — and, since the source stamps valid-time from the PG commit
     * time, its own valid-time version. */
    protected fun commitTx(sql: String, bindParams: (PreparedStatement) -> Unit) =
        pgConn().use { c ->
            c.createStatement().use { it.execute("BEGIN") }
            c.prepareStatement(sql).use { ps -> bindParams(ps); ps.executeUpdate() }
            c.createStatement().use { it.execute("COMMIT") }
        }

    protected fun dropSlot(slot: String) =
        pgExecute("SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name = '$slot'")

    // --- node ---

    protected fun openNode(
        logDir: Path, storageDir: Path,
        pg: PostgreSQLContainer = postgres,
        username: String = "testuser", password: String = "testpass",
    ): Xtdb = Xtdb.openNode {
        server { port = 0 }
        log(localLog(logDir))
        storage(Storage.local(storageDir))
        remote("pg", PostgresRemote.Factory(
            hostname = pg.host, port = pg.firstMappedPort,
            database = "testdb", username = username, password = password,
        ))
    }

    protected fun attachCdc(node: Xtdb, database: String, cdcLog: Path, cdcStorage: Path, slot: String, pub: String) {
        node.createConnectionBuilder().build().use { c ->
            c.createStatement().use { s ->
                s.execute(
                    """
                    ATTACH DATABASE $database WITH $$
                        storage: !Local
                          path: $cdcStorage
                        log: !Local
                          path: $cdcLog
                        externalSource: !Postgres
                          remote: pg
                          slotName: $slot
                          publicationName: $pub
                          indexer: !DirectMirror {}
                    $$""".trimIndent()
                )
            }
        }
    }

    protected fun xtQuery(node: Xtdb, database: String, sql: String): List<Map<String, Any?>> =
        node.createConnectionBuilder().database(database).build().use { c ->
            c.createStatement().use { s ->
                s.executeQuery(sql).use { rs ->
                    val cols = (1..rs.metaData.columnCount).map { rs.metaData.getColumnName(it) }
                    buildList { while (rs.next()) add(cols.associateWith { rs.getObject(it) }) }
                }
            }
        }

    // --- awaits ---

    protected suspend fun awaitCondition(description: String, timeout: Duration = 10.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (System.currentTimeMillis() < deadline) {
            if (check()) return
            runInterruptible { Thread.sleep(200) }
        }
        fail("Timed out waiting for: $description")
    }

    /** Polls `f` until `done` holds or the timeout elapses, returning the last value either way —
     * so a caller can assert on it and surface what actually showed up rather than a bare timeout. */
    protected suspend fun <T> await(timeout: Duration = 30.seconds, done: (T) -> Boolean, f: () -> T): T {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        var v = f()
        while (!done(v) && System.currentTimeMillis() < deadline) {
            runInterruptible { Thread.sleep(200) }
            v = f()
        }
        return v
    }

    protected suspend fun flushBlock(node: Xtdb) {
        val cdc = (node as XtdbInternal).dbCatalog["cdc"]!!
        val before = cdc.blockCatalog.currentBlockIndex
        cdc.sendFlushBlockMessage()
        // wait for *this* flush's block, not just any block — matters when flushing repeatedly
        awaitCondition("block persisted for cdc", timeout = 30.seconds) {
            cdc.blockCatalog.currentBlockIndex.let { it != null && (before == null || it > before) }
        }
    }

    /** Waits until the source has finished its initial snapshot and switched to streaming, so a
     *  write won't race the snapshot→stream handoff (a row committed ahead of the slot's consistent
     *  point would only be seen as the snapshot's current-state read, not streamed as its own
     *  valid-time version).
     *
     *  The source stamps each snapshot batch with a `snapshotCompleted = false` token and writes a
     *  final `snapshotCompleted = true` marker before streaming begins; the cdc db's watchers track
     *  the latest applied token, so `snapshotCompleted` going true means the whole snapshot is in
     *  (indexTx awaits application) and streaming is live. */
    protected suspend fun awaitStreaming(node: Xtdb) =
        // cdc may be momentarily absent right after a restart (it re-attaches from the replayed log),
        // so resolve it inside the poll rather than up front
        awaitCondition("cdc snapshot complete, streaming live", timeout = 30.seconds) {
            val cdc = (node as XtdbInternal).dbCatalog["cdc"]
            cdc?.watchers?.externalSourceToken?.let { PostgresSourceToken.parseFrom(it).snapshotCompleted } == true
        }

    companion object {
        // slots are dropped per run, but keep headroom across the whole property suite
        private val postgres = PostgreSQLContainer("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=50")
            .also { it.start() }
    }
}
