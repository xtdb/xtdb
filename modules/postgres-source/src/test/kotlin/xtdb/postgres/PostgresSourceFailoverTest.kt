package xtdb.postgres

import clojure.lang.Keyword
import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.error.Anomaly
import xtdb.api.error.Incorrect
import xtdb.postgres.proto.PostgresSourceToken
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
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

        /** Lazy so only the pre-PG17 refusal pays for it. */
        private val pg16: PostgreSQLContainer by lazy {
            PostgreSQLContainer("postgres:16-alpine")
                .withDatabaseName("testdb")
                .withUsername("testuser")
                .withPassword("testpass")
                .withCommand("postgres", "-c", "wal_level=logical")
                .also { it.start() }
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
            // gives the standby the ordinary client connection pg_sync_replication_slots() needs
            .withEnv("HA_PRIMARY_DB", "testdb")
            .withEnv("HA_PRIMARY_PASSWORD", "testpass")
            .withExposedPorts(5432)
            // it pg_basebackups itself from the primary before Postgres even starts
            .withStartupTimeout(Duration.ofMinutes(3))
            .waitingFor(Wait.forListeningPort())
            .dependsOn(primary)

        val standbyHost get() = standby.host
        val standbyPort get() = standby.getMappedPort(5432)
        val primaryHost get() = primary.host
        val primaryPort get() = primary.firstMappedPort

        fun start() {
            Startables.deepStart(primary, standby).join()
        }

        /** Copies the primary's `failover = true` slots onto the standby, and returns what landed. */
        fun syncSlots(): List<String?> {
            pgColumn(standbyHost, standbyPort, "SELECT pg_sync_replication_slots()")
            return pgColumn(standbyHost, standbyPort, "SELECT slot_name FROM pg_replication_slots")
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

    private fun conn(host: String, port: Int, database: String = "testdb"): Connection =
        DriverManager.getConnection("jdbc:postgresql://$host:$port/$database", "testuser", "testpass")

    private fun pgColumn(host: String, port: Int, sql: String): List<String?> =
        conn(host, port).use { c ->
            c.createStatement().use { s ->
                s.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
            }
        }

    private fun pgExec(host: String, port: Int, sql: String) =
        conn(host, port).use { c -> c.createStatement().use { it.execute(sql) } }

    /** Every refusal below is an [Incorrect], so the code is what distinguishes them. */
    private val Anomaly.errorCode: String?
        get() = (getData().valAt(Keyword.intern("xtdb.error", "code")) as? Keyword)?.toString()

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

    @Test
    fun `an operator-enabled failover slot survives promotion`() = runTest(timeout = 600.seconds) {
        val slot = unique("xtdb_slot")
        val pub = unique("xtdb_pub")
        val logDir = Files.createTempDirectory("sync-log")
        val storageDir = Files.createTempDirectory("sync-storage")
        val cdcLog = Files.createTempDirectory("sync-cdc-log")
        val cdcStorage = Files.createTempDirectory("sync-cdc-storage")

        HaPair(haImage).use { ha ->
            ha.start()

            pgExecute(
                ha.primary,
                "CREATE TABLE widgets (_id INT PRIMARY KEY, name TEXT)",
                "INSERT INTO widgets (_id, name) VALUES (1, 'snapshot-row')",
                "CREATE PUBLICATION $pub FOR TABLE widgets",
            )

            // the slot must be created *after* the standby is streaming, or its restart_lsn points
            // at WAL the standby never held and no copy can be made
            eventually(60.seconds) {
                assertEquals(
                    listOf("t"), pgColumn(ha.standbyHost, ha.standbyPort, "SELECT pg_is_in_recovery()"),
                    "standby is up and in recovery",
                )
            }

            openNode(logDir, storageDir, ha.primaryHost, ha.primaryPort).use { node ->
                attachCdc(node, "cdc", cdcLog, cdcStorage, slot, pub)
                awaitStreaming(node)

                assertEquals(
                    listOf("f"),
                    pgColumn(ha.primaryHost, ha.primaryPort, "SELECT failover FROM pg_replication_slots WHERE slot_name = '$slot'"),
                    "test precondition: XTDB creates its slot without failover",
                )
            }

            // ALTER_REPLICATION_SLOT blocks on an active slot rather than erroring, so the source
            // has to be stopped first — an unclean disconnect can hold it until wal_sender_timeout
            eventually(90.seconds) {
                assertEquals(
                    listOf("f"),
                    pgColumn(ha.primaryHost, ha.primaryPort, "SELECT active FROM pg_replication_slots WHERE slot_name = '$slot'"),
                    "slot released once the node closed",
                )
            }

            val flip = FailoverSlot.enable(pgRemote(ha.primaryHost, ha.primaryPort), slot)

            assertIs<FailoverSlot.Outcome.Enabled>(flip)
            assertEquals(
                listOf("t"),
                pgColumn(ha.primaryHost, ha.primaryPort, "SELECT failover FROM pg_replication_slots WHERE slot_name = '$slot'"),
                "the operator's ALTER took effect",
            )

            openNode(logDir, storageDir, ha.primaryHost, ha.primaryPort).use { node ->
                // consuming keeps restart_lsn moving with the standby; a frozen slot can't be copied
                pgExecute(ha.primary, "INSERT INTO widgets (_id, name) VALUES (2, 'after-alter')")
                eventually(30.seconds) {
                    assertTrue(
                        xtQuery(node, "cdc", "SELECT _id FROM public.widgets WHERE _id = 2").isNotEmpty(),
                        "source resumed against the upgraded slot",
                    )
                }

                eventually(60.seconds) {
                    assertEquals(listOf(slot), ha.syncSlots(), "slot copied to the standby")
                }

                ha.promote()
                ha.primary.stop()
            }

            assertEquals(
                listOf("t", "t"),
                pgColumn(ha.standbyHost, ha.standbyPort, "SELECT failover FROM pg_replication_slots WHERE slot_name = '$slot'") +
                    pgColumn(ha.standbyHost, ha.standbyPort, "SELECT synced FROM pg_replication_slots WHERE slot_name = '$slot'"),
                "the slot survived promotion, unlike the unflagged one",
            )

            openNode(logDir, storageDir, ha.standbyHost, ha.standbyPort).use { node ->
                pgExec(ha.standbyHost, ha.standbyPort, "INSERT INTO widgets (_id, name) VALUES (3, 'after-failover')")

                // the point of the whole exercise: writes to the new primary still reach XT
                eventually(90.seconds) {
                    assertTrue(
                        xtQuery(node, "cdc", "SELECT _id FROM public.widgets WHERE _id = 3").isNotEmpty(),
                        "a row written to the promoted primary reaches XT",
                    )
                }

                assertNull(
                    (node as XtdbInternal).dbCatalog["cdc"]?.ingestionError,
                    "the source resumed cleanly against the promoted standby",
                )
                assertPrimaryDbHealthy(node)
            }
        }
    }

    // --- the tool, against a single server; the promotion path is the test above ---

    @Test
    fun `enables failover on an idle slot`() {
        val remote = pgRemote()
        val slot = unique("tool_slot")
        pgExecute("SELECT pg_create_logical_replication_slot('$slot', 'pgoutput')")

        try {
            val outcome = assertIs<FailoverSlot.Outcome.Enabled>(FailoverSlot.enable(remote, slot))

            assertFalse(outcome.before.failover, "test precondition: slots are created without failover")
            assertTrue(outcome.after.failover)
            assertEquals(
                listOf("t"),
                pgColumn(remote.hostname, remote.port, "SELECT failover FROM pg_replication_slots WHERE slot_name = '$slot'"),
                "Postgres agrees, rather than us reporting back our own intent",
            )
        } finally {
            dropSlot(slot)
        }
    }

    @Test
    fun `reports a slot that already has failover enabled`() {
        val remote = pgRemote()
        val slot = unique("tool_slot")
        pgExecute("SELECT pg_create_logical_replication_slot('$slot', 'pgoutput')")

        try {
            FailoverSlot.enable(remote, slot)

            assertIs<FailoverSlot.Outcome.AlreadyEnabled>(FailoverSlot.enable(remote, slot))
        } finally {
            dropSlot(slot)
        }
    }

    @Test
    fun `refuses a slot the source is still holding`() = runTest(timeout = 180.seconds) {
        val remote = pgRemote()
        val slot = unique("tool_slot")
        val pub = unique("tool_pub")
        val table = unique("tool_widgets")
        val logDir = Files.createTempDirectory("tool-log")
        val storageDir = Files.createTempDirectory("tool-storage")
        val cdcLog = Files.createTempDirectory("tool-cdc-log")
        val cdcStorage = Files.createTempDirectory("tool-cdc-storage")

        pgExecute("CREATE TABLE $table (_id INT PRIMARY KEY)", "CREATE PUBLICATION $pub FOR TABLE $table")

        try {
            openNode(logDir, storageDir).use { node ->
                attachCdc(node, "cdc", cdcLog, cdcStorage, slot, pub)
                awaitStreaming(node)

                assertEquals(
                    listOf("t"),
                    pgColumn(remote.hostname, remote.port, "SELECT active FROM pg_replication_slots WHERE slot_name = '$slot'"),
                    "test precondition: the source is holding the slot",
                )

                val error = assertThrows<Incorrect> { FailoverSlot.enable(remote, slot) }
                assertEquals(":xtdb.postgres/slot-active", error.errorCode)
                assertTrue(
                    error.message!!.contains(slot),
                    "expected the refusal to name the slot, got: ${error.message}",
                )
            }
        } finally {
            runCatching { dropSlot(slot) }
            pgExecute("DROP PUBLICATION IF EXISTS $pub", "DROP TABLE IF EXISTS $table")
        }
    }

    /** Postgres scopes neither the command nor the error by database — the flip succeeds against a
     * slot the remote has nothing to do with, so a mistyped name silently alters someone else's. */
    @Test
    fun `refuses a slot belonging to another database`() {
        val remote = pgRemote()
        val otherDb = unique("tool_db")
        val slot = unique("tool_slot")

        pgExecute("CREATE DATABASE $otherDb")

        try {
            conn(remote.hostname, remote.port, otherDb).use { c ->
                c.createStatement().use { it.execute("SELECT pg_create_logical_replication_slot('$slot', 'pgoutput')") }
            }

            val error = assertThrows<Incorrect> { FailoverSlot.enable(remote, slot) }
            assertEquals(":xtdb.postgres/slot-wrong-database", error.errorCode)
            assertTrue(
                error.message!!.contains(otherDb) && error.message!!.contains(remote.database),
                "expected the refusal to name both databases, got: ${error.message}",
            )
        } finally {
            dropSlot(slot)
            pgExecute("DROP DATABASE IF EXISTS $otherDb")
        }
    }

    // --- resolving the slot from node config, as `pg-enable-slot-failover` does ---

    private fun nodeConfig(remote: PostgresRemote): Path =
        Files.createTempFile("failover-config", ".yaml").also {
            Files.writeString(
                it,
                """
                remotes:
                  pg: !Postgres
                    hostname: ${remote.hostname}
                    port: ${remote.port}
                    database: ${remote.database}
                    username: ${remote.username}
                    password: ${remote.password}
                """.trimIndent()
            )
        }

    @Test
    fun `enables failover on the slot named by a remote alias in node config`() {
        val remote = pgRemote()
        val slot = unique("tool_slot")
        val config = nodeConfig(remote)
        pgExecute("SELECT pg_create_logical_replication_slot('$slot', 'pgoutput')")

        try {
            val outcome = assertIs<FailoverSlot.Outcome.Enabled>(FailoverSlot.enable(config, "pg", slot))

            assertEquals("${remote.hostname}:${remote.port}/${remote.database}", outcome.server)
            assertEquals(
                listOf("t"),
                pgColumn(remote.hostname, remote.port, "SELECT failover FROM pg_replication_slots WHERE slot_name = '$slot'"),
                "the aliased remote's slot was the one altered",
            )
            assertTrue(
                FailoverSlot.report(outcome).contains("Enabled failover on replication slot '$slot'"),
                "the operator is told which slot changed, got: ${FailoverSlot.report(outcome)}",
            )
        } finally {
            dropSlot(slot)
            Files.deleteIfExists(config)
        }
    }

    @Test
    fun `refuses an alias the node config doesn't define`() {
        val config = nodeConfig(pgRemote())

        try {
            val error = assertThrows<Incorrect> { FailoverSlot.enable(config, "nope", "irrelevant_slot") }
            assertEquals(":xtdb.postgres/missing-remote", error.errorCode)
            assertTrue(
                error.message!!.contains("pg"),
                "expected the refusal to list the aliases that are defined, got: ${error.message}",
            )
        } finally {
            Files.deleteIfExists(config)
        }
    }

    /** `ALTER_REPLICATION_SLOT` arrived with failover slots in PG17. Before that the server answers
     * `syntax error at or near "ALTER_REPLICATION_SLOT"`, which reads as a bug in XTDB's SQL. */
    @Test
    fun `refuses a server predating failover slots`() {
        val slot = unique("tool_slot")
        pgExecute(pg16, "SELECT pg_create_logical_replication_slot('$slot', 'pgoutput')")

        try {
            val error = assertThrows<Incorrect> { FailoverSlot.enable(pgRemote(pg16), slot) }
            assertEquals(":xtdb.postgres/failover-slots-unsupported", error.errorCode)
            assertTrue(
                error.message!!.contains("PostgreSQL 17") && error.message!!.endsWith("is 16"),
                "expected the refusal to name the required and actual versions, got: ${error.message}",
            )
        } finally {
            pgExecute(pg16, "SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name = '$slot'")
        }
    }
}
