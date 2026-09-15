package xtdb.postgres

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.Network
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The ways an upstream takes the replication connection away, and whether ingestion survives them (#5878).
 * Each case asserts that a row written *after* the loss reaches XT, rather than asserting on the exception.
 */
@Tag("integration")
class PostgresSourceConnectionLossTest : PostgresSourceTestBase() {

    private fun column(conn: Connection, sql: String): List<String?> =
        conn.use { c ->
            c.createStatement().use { s ->
                s.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
            }
        }

    private fun pgColumn(sql: String) = column(pgConn(), sql)

    private fun conn(host: String, port: Int): Connection =
        DriverManager.getConnection("jdbc:postgresql://$host:$port/testdb", "testuser", "testpass")

    private fun cdcError(node: Xtdb) = (node as XtdbInternal).dbCatalog["cdc"]?.ingestionError

    @Test
    fun `ingestion survives the walsender being killed under a live stream`() = runTest(timeout = 300.seconds) {
        val slot = unique("slot")
        val pub = unique("pub")
        val table = unique("widgets")
        val dirs = List(4) { Files.createTempDirectory("conn-loss") }

        pgExecute(
            "CREATE TABLE $table (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO $table (_id, name) VALUES (1, 'snapshot-row')",
            "CREATE PUBLICATION $pub FOR TABLE $table",
        )

        try {
            openNode(dirs[0], dirs[1]).use { node ->
                attachCdc(node, "cdc", dirs[2], dirs[3], slot, pub)
                awaitStreaming(node)

                pgExecute("INSERT INTO $table (_id, name) VALUES (2, 'before-kill')")
                eventually(30.seconds) {
                    assertTrue(
                        xtQuery(node, "cdc", "SELECT _id FROM public.$table WHERE _id = 2").isNotEmpty(),
                        "streaming before the kill",
                    )
                }

                assertNotNull(
                    pgColumn("SELECT active_pid FROM pg_replication_slots WHERE slot_name = '$slot'").single(),
                    "test precondition: a walsender holds the slot",
                )

                assertEquals(
                    listOf("t"),
                    pgColumn(
                        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots" +
                            " WHERE slot_name = '$slot' AND active_pid IS NOT NULL",
                    ),
                    "the walsender is killed under the stream",
                )

                pgExecute("INSERT INTO $table (_id, name) VALUES (3, 'after-kill')")

                eventually(120.seconds) {
                    assertNull(cdcError(node), "the source reconnects rather than failing")
                    assertTrue(
                        xtQuery(node, "cdc", "SELECT _id FROM public.$table WHERE _id = 3").isNotEmpty(),
                        "a row written after the kill reaches XT",
                    )
                }
            }
        } finally {
            runCatching { dropSlot(slot) }
            dirs.forEach { it.toFile().deleteRecursively() }
        }
    }

    @Test
    fun `ingestion survives the upstream restarting`() = runTest(timeout = 600.seconds) {
        val slot = unique("slot")
        val pub = unique("pub")
        val dirs = List(4) { Files.createTempDirectory("conn-loss-restart") }
        val network = Network.newNetwork()

        // its own server, because a restart takes down every connection to it
        val pg = PostgreSQLContainer("postgres:17-alpine")
            .withNetwork(network)
            .withNetworkAliases("upstream")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical")

        pg.start()

        // Everything — the node and this test's own writes — goes through the proxy. A docker restart
        // republishes the container's port, so the address testcontainers handed us before the restart is
        // dead afterwards; the proxy's own port doesn't move, and its target is a network alias that
        // resolves to whatever the restarted container became.
        PgProxy(network, "upstream").use { proxy ->
            try {
                conn(proxy.host, proxy.port).use { c ->
                    c.createStatement().use { s ->
                        s.execute("CREATE TABLE widgets (_id INT PRIMARY KEY, name TEXT)")
                        s.execute("INSERT INTO widgets (_id, name) VALUES (1, 'snapshot-row')")
                        s.execute("CREATE PUBLICATION $pub FOR TABLE widgets")
                    }
                }

                openNode(dirs[0], dirs[1], proxy.host, proxy.port).use { node ->
                    attachCdc(node, "cdc", dirs[2], dirs[3], slot, pub)
                    awaitStreaming(node)

                    conn(proxy.host, proxy.port).use { c ->
                        c.createStatement().use { it.execute("INSERT INTO widgets (_id, name) VALUES (2, 'before-restart')") }
                    }
                    eventually(30.seconds) {
                        assertTrue(
                            xtQuery(node, "cdc", "SELECT _id FROM public.widgets WHERE _id = 2").isNotEmpty(),
                            "streaming before the restart",
                        )
                    }

                    pg.dockerClient.restartContainerCmd(pg.containerId).exec()

                    eventually(120.seconds) {
                        assertEquals(
                            listOf("1"),
                            column(conn(proxy.host, proxy.port), "SELECT 1"),
                            "the upstream is accepting again",
                        )
                    }

                    conn(proxy.host, proxy.port).use { c ->
                        c.createStatement().use { it.execute("INSERT INTO widgets (_id, name) VALUES (3, 'after-restart')") }
                    }

                    // inside the ~168s the seven backoffs take to exhaust
                    eventually(150.seconds) {
                        assertNull(cdcError(node), "the source reconnects rather than failing")
                        assertTrue(
                            xtQuery(node, "cdc", "SELECT _id FROM public.widgets WHERE _id = 3").isNotEmpty(),
                            "a row written after the restart reaches XT",
                        )
                    }
                }
            } finally {
                runCatching { pg.stop() }
                runCatching { network.close() }
                dirs.forEach { it.toFile().deleteRecursively() }
            }
        }
    }
}
