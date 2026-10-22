package xtdb.mysql

import io.kotest.assertions.nondeterministic.eventually
import org.testcontainers.mysql.MySQLContainer
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.log.Log.Companion.localLog
import xtdb.api.storage.Storage
import xtdb.mysql.proto.MySqlSourceToken
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The tests' MySQL: a testcontainers one, or — on a machine with no Docker — one already running, named by
 * `XTDB_TEST_MYSQL_HOST` (and `_PORT`, `_USER`, `_PASSWORD`).
 *
 * The tests set globals and purge binlogs on it, so an already-running one MUST be disposable, and set up as the
 * source requires (`gtid_mode=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`).
 */
data class Upstream(val host: String, val port: Int, val user: String, val password: String)

fun xtdb.api.error.Anomaly.errorCode(): String? =
    data.valAt(clojure.lang.Keyword.intern("xtdb.error", "code"))?.toString()?.removePrefix(":")

abstract class MySqlSourceTestBase {

    protected fun mysqlConn(): Connection =
        DriverManager.getConnection("jdbc:mysql://${upstream.host}:${upstream.port}/", upstream.user, upstream.password)

    protected fun mysqlExecute(vararg statements: String) =
        mysqlConn().use { c -> c.createStatement().use { s -> statements.forEach { s.execute(it) } } }

    protected fun mysqlQuery(sql: String): List<Map<String, Any?>> =
        mysqlConn().use { c ->
            c.createStatement().use { s ->
                s.executeQuery(sql).use { rs ->
                    val cols = (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
                    buildList { while (rs.next()) add(cols.associateWith { rs.getObject(it) }) }
                }
            }
        }

    protected fun unique(prefix: String) = "${prefix}_${UUID.randomUUID().toString().replace("-", "").take(12)}"

    protected fun mysqlTx(vararg statements: String) =
        mysqlConn().use { c ->
            c.autoCommit = false
            c.createStatement().use { s -> statements.forEach { s.execute(it) } }
            c.commit()
        }

    protected fun serverId() = Random.nextLong(10_000, 2_000_000)

    protected fun openNode(
        logDir: Path, storageDir: Path,
        upstream: Upstream = Companion.upstream,
    ): Xtdb = Xtdb.openNode {
        server { port = 0 }
        log(localLog(logDir))
        storage(Storage.local(storageDir))
        remote("mysql", MySqlRemote.Factory(
            hostname = upstream.host, port = upstream.port,
            username = upstream.user, password = upstream.password,
            heartbeatInterval = 1.seconds,
        ))
    }

    protected fun attachCdc(
        node: Xtdb, database: String, cdcLog: Path, cdcStorage: Path,
        serverId: Long, tables: List<String>, indexer: String = "!DirectMirror {}",
    ) {
        node.createConnectionBuilder().build().use { c ->
            c.createStatement().use { s ->
                s.execute(
                    """
                    ATTACH DATABASE $database WITH $$
                        storage: !Local
                          path: $cdcStorage
                        log: !Local
                          path: $cdcLog
                        externalSource: !MySQL
                          remote: mysql
                          serverId: $serverId
                          tables: [${tables.joinToString()}]
                          indexer: $indexer
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

    protected suspend fun awaitStreaming(node: Xtdb, database: String = "cdc") =
        eventually(30.seconds) {
            val db = (node as XtdbInternal).dbCatalog[database]
            assertTrue(
                db?.watchers?.externalSourceToken?.let { MySqlSourceToken.parseFrom(it).snapshotCompleted } == true,
                "$database snapshot complete, streaming live"
            )
        }

    protected fun appliedPosition(node: Xtdb, database: String = "cdc"): GtidSet? =
        (node as XtdbInternal).dbCatalog[database]?.watchers?.externalSourceToken
            ?.let { GtidSet.parse(MySqlSourceToken.parseFrom(it).gtidSet) }

    companion object {
        val upstream: Upstream by lazy {
            System.getenv("XTDB_TEST_MYSQL_HOST")?.let { host ->
                Upstream(
                    host,
                    System.getenv("XTDB_TEST_MYSQL_PORT")?.toInt() ?: 3306,
                    System.getenv("XTDB_TEST_MYSQL_USER") ?: "root",
                    System.getenv("XTDB_TEST_MYSQL_PASSWORD") ?: "",
                )
            } ?: MySQLContainer("mysql:8.0")
                .withCommand(
                    "--server-id=1", "--log-bin=mysql-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                    "--binlog-format=ROW", "--binlog-row-image=FULL", "--binlog-row-metadata=FULL",
                )
                .also { it.start() }
                // the container's own `root` carries the global privileges FTWRL and the binlog dump need
                .let { Upstream(it.host, it.firstMappedPort, "root", it.password) }
        }
    }
}
