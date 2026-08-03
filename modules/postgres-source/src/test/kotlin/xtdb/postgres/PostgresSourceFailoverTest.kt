package xtdb.postgres

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.postgres.proto.PostgresSourceToken
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Failover scenarios for the Postgres source — what it does when the server it resumes against
 * isn't the one it left.
 *
 * The primary/standby pair comes from `src/test/resources/xtdb/postgres/ha`, whose README covers
 * how it's built and why a promoted standby holds no logical replication slot. The production
 * incident behind this, and PG17 failover slots, are #5828.
 */
@Tag("integration")
class PostgresSourceFailoverTest : PostgresSourceTestBase() {

    companion object {
        private const val HA_RESOURCES = "xtdb/postgres/ha"

        private val haImage: DockerImageName by lazy {
            val tag = ImageFromDockerfile()
                .withFileFromClasspath("Dockerfile", "$HA_RESOURCES/Dockerfile")
                .withFileFromClasspath("ha-entrypoint.sh", "$HA_RESOURCES/ha-entrypoint.sh")
                .withFileFromClasspath("ha-promote.sh", "$HA_RESOURCES/ha-promote.sh")
                .withFileFromClasspath("primary-initdb.sh", "$HA_RESOURCES/primary-initdb.sh")
                .get()

            DockerImageName.parse(tag).asCompatibleSubstituteFor("postgres")
        }
    }

    private inner class HaPair(image: DockerImageName) : AutoCloseable {
        val network: Network = Network.newNetwork()

        val primary: PostgreSQLContainer = PostgreSQLContainer(image)
            .withNetwork(network)
            .withNetworkAliases("primary")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")

        val standby: GenericContainer<*> = GenericContainer(image)
            .withNetwork(network)
            .withEnv("HA_ROLE", "standby")
            .withEnv("HA_PRIMARY_HOST", "primary")
            .withEnv("HA_REPLICATION_USER", "testuser")
            // PG17 slot synchronisation requires the standby to hold a physical slot
            .withEnv("HA_PRIMARY_SLOT", "standby_slot")
            .withExposedPorts(5432)
            // it pg_basebackups itself from the primary before Postgres even starts
            .withStartupTimeout(Duration.ofMinutes(3))
            .waitingFor(Wait.forListeningPort())
            .dependsOn(primary)

        fun start() {
            Startables.deepStart(primary, standby).join()
        }

        /** Promotes only once the standby has replayed everything the primary has written.
         *
         * The fence needs no table of its own, so it holds whatever the caller has created;
         * pgoutput only forwards messages when asked, so it stays invisible to a source reading
         * the slot. */
        suspend fun promote() {
            val fence = pgColumn(
                primary.host, primary.firstMappedPort,
                "SELECT pg_logical_emit_message(false, 'xtdb-ha-fence', '')",
            ).single()

            eventually(60.seconds) {
                assertEquals(
                    listOf("t"),
                    pgColumn(
                        standby.host, standby.getMappedPort(5432),
                        "SELECT pg_last_wal_replay_lsn() >= '$fence'::pg_lsn",
                    ),
                    "standby has replayed up to the fence at $fence",
                )
            }

            val result = standby.execInContainer("ha-promote.sh")
            assertEquals(0, result.exitCode, "ha-promote.sh failed: ${result.stdout}${result.stderr}")
        }

        override fun close() {
            runCatching { standby.stop() }
            runCatching { primary.stop() }
            runCatching { network.close() }
        }
    }

    private fun conn(host: String, port: Int): Connection =
        DriverManager.getConnection("jdbc:postgresql://$host:$port/testdb", "testuser", "testpass")

    private fun pgColumn(host: String, port: Int, sql: String): List<String?> =
        conn(host, port).use { c ->
            c.createStatement().use { s ->
                s.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
            }
        }

    private fun latestToken(node: Xtdb): PostgresSourceToken? =
        (node as XtdbInternal).dbCatalog["cdc"]?.watchers?.externalSourceToken
            ?.let { PostgresSourceToken.parseFrom(it) }

    /** A dead external source must take down its own database and nothing else. */
    private fun assertPrimaryDbHealthy(node: Xtdb) {
        val id = unique("health")
        node.createConnectionBuilder().database("xtdb").build().use { c ->
            c.createStatement().use { s ->
                s.execute("INSERT INTO primary_health (_id, v) VALUES ('$id', 'ok')")
                s.executeQuery("SELECT v FROM primary_health WHERE _id = '$id'").use { rs ->
                    assertTrue(rs.next(), "primary insert should be visible")
                    assertEquals("ok", rs.getString("v"))
                }
            }
        }
    }

    @Test
    fun `resuming against a promoted standby surfaces an ingestion error`() = runTest(timeout = 600.seconds) {
        val slot = unique("xtdb_slot")
        val pub = unique("xtdb_pub")
        val logDir = Files.createTempDirectory("failover-log")
        val storageDir = Files.createTempDirectory("failover-storage")
        val cdcLog = Files.createTempDirectory("failover-cdc-log")
        val cdcStorage = Files.createTempDirectory("failover-cdc-storage")

        HaPair(haImage).use { ha ->
            ha.start()

            val primaryHost = ha.primary.host
            val primaryPort = ha.primary.firstMappedPort
            val standbyHost = ha.standby.host
            val standbyPort = ha.standby.getMappedPort(5432)

            pgExecute(
                ha.primary,
                "CREATE TABLE widgets (_id INT PRIMARY KEY, name TEXT)",
                "INSERT INTO widgets (_id, name) VALUES (1, 'snapshot-row')",
                "CREATE PUBLICATION $pub FOR TABLE widgets",
            )

            eventually(60.seconds) {
                assertEquals(
                    listOf("t"), pgColumn(standbyHost, standbyPort, "SELECT pg_is_in_recovery()"),
                    "standby is up and in recovery",
                )
            }

            openNode(logDir, storageDir, primaryHost, primaryPort).use { node ->
                attachCdc(node, "cdc", cdcLog, cdcStorage, slot, pub)
                awaitStreaming(node)

                // advances the token past the snapshot's consistent point, so the reopen below
                // takes the resume path rather than re-snapshotting
                pgExecute(ha.primary, "INSERT INTO widgets (_id, name) VALUES (2, 'streamed-row')")
                eventually(30.seconds) {
                    assertTrue(
                        xtQuery(node, "cdc", "SELECT _id FROM public.widgets WHERE _id = 2").isNotEmpty(),
                        "streamed row mirrored",
                    )
                }

                assertTrue(latestToken(node)?.snapshotCompleted == true, "test precondition: resume path")
                assertEquals(
                    listOf(slot),
                    pgColumn(primaryHost, primaryPort, "SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'logical'"),
                    "test precondition: our slot exists before the failover",
                )

                // the lagging-standby case is a different failure, blocked on #5828
                ha.promote()
                ha.primary.stop()
            }

            // asserted so the test can't pass off the back of a different failure — a missing
            // publication throws `Incorrect` from another path and would look the same outside
            assertEquals(listOf("f"), pgColumn(standbyHost, standbyPort, "SELECT pg_is_in_recovery()"))
            assertEquals(
                listOf("1", "2"),
                pgColumn(standbyHost, standbyPort, "SELECT _id FROM widgets ORDER BY _id"),
                "rows survive the failover — Postgres loses no committed data",
            )
            assertEquals(
                listOf("1"),
                pgColumn(standbyHost, standbyPort, "SELECT count(*) FROM pg_publication WHERE pubname = '$pub'"),
                "the publication survives — ordinary catalog, physically replicated",
            )
            assertEquals(
                emptyList<String?>(),
                pgColumn(standbyHost, standbyPort, "SELECT slot_name FROM pg_replication_slots"),
                "but no slot does — pg_replslot is neither WAL-logged nor base-backed-up",
            )

            openNode(logDir, storageDir, standbyHost, standbyPort).use { node ->
                val dbs = (node as XtdbInternal).dbCatalog

                // the disabled `slot recreation silently drops changes` in
                // PostgresSourceIntegrationTest is what failing loudly here protects against
                val error = eventually(60.seconds) {
                    assertNotNull(
                        dbs["cdc"]?.ingestionError,
                        "a missing slot must stop ingestion, not resume from the new server's position",
                    )
                }

                val rendered = error.stackTraceToString()
                assertTrue(
                    rendered.contains(slot) && rendered.contains("does not exist"),
                    "expected the failure to name the missing slot '$slot', got: $rendered",
                )

                assertPrimaryDbHealthy(node)
            }
        }
    }
}
