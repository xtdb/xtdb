package xtdb.postgres

import clojure.lang.ILookup
import clojure.lang.Keyword
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.postgres.proto.PostgresSourceToken
import org.postgresql.replication.LogSequenceNumber
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.error.Conflict
import xtdb.api.log.KafkaCluster
import xtdb.time.Interval
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import io.kotest.assertions.nondeterministic.continually
import io.kotest.assertions.nondeterministic.eventually

@Tag("integration")
class PostgresSourceIntegrationTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val postgres = PostgreSQLContainer("postgres:17-alpine")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            // tests share this one container and don't drop their slots, so raise the
            // ceiling above the default 10 to leave headroom as the suite grows
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=50", "-c", "max_wal_senders=50")

        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            Startables.deepStart(postgres, kafka).join()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            postgres.stop()
            kafka.stop()
            network.close()
        }
    }

    private fun PostgreSQLContainer.executeSql(vararg statements: String) {
        DriverManager.getConnection(jdbcUrl, username, password).use { conn ->
            conn.createStatement().use { stmt ->
                for (sql in statements) stmt.execute(sql)
            }
        }
    }

    private fun pgExecute(vararg statements: String) = postgres.executeSql(*statements)

    private fun newDedicatedPostgres(image: String = "postgres:17-alpine"): PostgreSQLContainer =
        PostgreSQLContainer(image)
            .withNetwork(network)
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical")

    private fun openNode(
        sourceTopic: String,
        pgContainer: PostgreSQLContainer = postgres,
        pgPassword: String = pgContainer.password,
    ): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
        remote("pg", PostgresRemote.Factory(
            hostname = pgContainer.host,
            port = pgContainer.getMappedPort(5432),
            database = pgContainer.databaseName,
            username = pgContainer.username,
            password = pgPassword,
        ))
        log(KafkaCluster.LogFactory("kafka", sourceTopic))
    }

    private fun attachPostgresSource(
        node: Xtdb,
        dbName: String = "cdc",
        slotName: String = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}",
        publicationName: String = "test_pub",
    ) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    ATTACH DATABASE $dbName WITH $$
                        log: !Kafka
                          cluster: kafka
                          topic: test-replica-${UUID.randomUUID()}
                        externalSource: !Postgres
                          remote: pg
                          slotName: $slotName
                          publicationName: $publicationName
                          indexer: !DirectMirror {}
                    $$""".trimIndent()
                )
            }
        }
    }

    /** Attaches a Postgres source whose secondary keeps a durable (local-disk) block
     * catalog, so a flushed block survives a node restart. */
    private fun attachPostgresSourceWithLocalStorage(
        node: Xtdb,
        storagePath: Path,
        dbName: String = "cdc",
        logTopic: String = "test-replica-${UUID.randomUUID()}",
        slotName: String = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}",
        publicationName: String = "test_pub",
    ) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    ATTACH DATABASE $dbName WITH $$
                        storage: !Local
                          path: $storagePath
                        log: !Kafka
                          cluster: kafka
                          topic: $logTopic
                        externalSource: !Postgres
                          remote: pg
                          slotName: $slotName
                          publicationName: $publicationName
                          indexer: !DirectMirror {}
                    $$""".trimIndent()
                )
            }
        }
    }

    private fun attachReadOnlyReplica(node: Xtdb, dbName: String, logTopic: String, storagePath: Path) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    ATTACH DATABASE $dbName WITH $$
                        storage: !Local
                          path: $storagePath
                        log: !Kafka
                          cluster: kafka
                          topic: $logTopic
                        mode: read-only
                    $$""".trimIndent()
                )
            }
        }
    }

    private fun xtQueryDb(node: Xtdb, dbName: String, sql: String): List<Map<String, Any?>> {
        return node.createConnectionBuilder().database(dbName).build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    val metadata = rs.metaData
                    val cols = (1..metadata.columnCount).map { metadata.getColumnName(it) }
                    buildList {
                        while (rs.next()) {
                            add(cols.associateWith { rs.getObject(it) })
                        }
                    }
                }
            }
        }
    }

    private fun assertPrimaryDbHealthy(node: Xtdb) {
        val id = UUID.randomUUID().toString()
        node.createConnectionBuilder().database("xtdb").build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("INSERT INTO primary_health (_id, v) VALUES ('$id', 'ok')")
                stmt.executeQuery("SELECT v FROM primary_health WHERE _id = '$id'").use { rs ->
                    assertTrue(rs.next(), "primary insert should be visible")
                    assertEquals("ok", rs.getString("v"))
                }
            }
        }
    }

    private suspend fun awaitTxs(node: Xtdb, expected: Int, db: String = "cdc", timeout: Duration = 5.seconds) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        var count = 0L
        while (System.currentTimeMillis() < deadline) {
            count = xtQueryDb(node, db, "SELECT count(*) AS cnt FROM xt.txs")[0]["cnt"] as Long
            if (count >= expected) return
            runInterruptible { Thread.sleep(200) }
        }
        throw AssertionError("Timed out waiting for $expected txs on db '$db' (got $count)")
    }

    /** Reads the most-recently applied source token from the live index (no flush required).
     *  Reflects the *last tx the source committed* — useful for asserting the source's
     *  progress against the snapshotCompleted flag. */
    private fun latestPostgresToken(node: Xtdb, db: String = "cdc"): PostgresSourceToken {
        val bytes = (node as XtdbInternal).dbCatalog[db]?.watchers?.externalSourceToken
            ?: fail("db '$db' has not applied any source tx yet — no token to read")
        return PostgresSourceToken.parseFrom(bytes)
    }

    @Test
    fun `resume from token after restart`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_resume (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_resume (_id, name) VALUES (1, 'Alice')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_resume",
        )

        // Phase 1: snapshot + streaming
        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            // 1 table batch + 1 completion marker = 2 txs
            awaitTxs(node, 2, db = "cdc")

            val snapshotRows = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.pg_resume ORDER BY _id")
            assertEquals(1, snapshotRows.size)
            assertEquals("Alice", snapshotRows[0]["name"])

            // Stream an insert so the token advances beyond the snapshot LSN
            pgExecute("INSERT INTO pg_resume (_id, name) VALUES (2, 'Bob')")

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_resume WHERE _id = 2").isNotEmpty(), "Bob appears")
            }
        }

        // Insert while node is down — this must appear after restart
        pgExecute("INSERT INTO pg_resume (_id, name) VALUES (3, 'Charlie')")

        // Phase 2: restart with the same Kafka source topic.
        // The node replays the ATTACH DATABASE from the source log,
        // recovers the CDC token (snapshotCompleted=true), and resumes streaming.
        openNode(sourceTopic).use { node ->
            eventually(60.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_resume WHERE _id = 3").isNotEmpty(), "Charlie appears after restart")
            }

            val rows = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.pg_resume ORDER BY _id")
            assertEquals(3, rows.size, "All three rows should be present — no duplication, no loss")
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])
            assertEquals("Charlie", rows[2]["name"])
        }
    }

    @Test
    fun `slot advances before the first block is cut`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_no_block (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_no_block (_id, name) VALUES (1, 'snapshot-row')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_no_block",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)
            awaitTxs(node, 2, db = "cdc")

            val cdc = (node as XtdbInternal).dbCatalog["cdc"]!!
            assertNull(cdc.blockCatalog.currentBlockIndex, "test precondition: no block cut")

            val atAttach = pgSlotConfirmedFlushLsn(slotName)

            pgExecute("INSERT INTO pg_no_block (_id, name) VALUES (2, 'streamed-two')")
            eventually(30.seconds) {
                assertEquals(2, xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_no_block").size, "streamed row applied")
            }

            eventually(30.seconds) {
                assertTrue(
                    pgSlotConfirmedFlushLsn(slotName) > atAttach,
                    "slot advances with no block to gate on, so Postgres can recycle unrelated WAL and copy the slot to a standby",
                )
            }
        }
    }

    @Test
    fun `slot is confirmed only as far as the last durable block`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_confirm (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_confirm (_id, name) VALUES (1, 'snapshot-row')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_confirm",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)
            awaitTxs(node, 2, db = "cdc")

            val cdc = (node as XtdbInternal).dbCatalog["cdc"]!!

            cdc.sendFlushBlockMessage()
            eventually(30.seconds) { assertTrue(cdc.blockCatalog.currentBlockIndex != null, "first block persisted") }
            val blockLsn = PostgresSourceToken.parseFrom(cdc.blockCatalog.externalSourceToken!!).latestCommittedLsn

            pgExecute(
                "INSERT INTO pg_confirm (_id, name) VALUES (2, 'streamed-two')",
                "INSERT INTO pg_confirm (_id, name) VALUES (3, 'streamed-three')",
            )
            eventually(30.seconds) {
                assertEquals(3, xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_confirm").size, "streamed rows applied")
            }

            // Applied but unblocked, so the leader has imported past the block — which also rules out the
            // idle walEnd advance, since that requires everything imported to be in a block. Confirming
            // further here would tell Postgres to recycle WAL for transactions whose only copies are the
            // replica log and the leader's live index.
            //
            // `continually`, not a single read: the ack reaches the slot asynchronously, so one sample taken
            // straight after the rows apply passes whether or not the gate is there. Holding the invariant
            // over a window is what makes it fail when confirmation tracks apply.
            continually(5.seconds) {
                val confirmed = pgSlotConfirmedFlushLsn(slotName)
                assertTrue(confirmed <= blockLsn, "slot confirmed at $confirmed, past the last durable block at $blockLsn")
            }

            cdc.sendFlushBlockMessage()
            eventually(30.seconds) {
                val persisted = PostgresSourceToken.parseFrom(cdc.blockCatalog.externalSourceToken!!).latestCommittedLsn
                assertTrue(persisted > blockLsn, "second block's token covers the streamed rows")
            }

            eventually(30.seconds) {
                assertTrue(pgSlotConfirmedFlushLsn(slotName) > blockLsn, "slot advances once the block is durable")
            }
        }
    }

    /**
     * Postgres forwards a start position below `confirmed_flush_lsn` up to the slot's own without
     * erroring, so the block's token is a lower bound on where we resume rather than the resume
     * point — `ConfirmedIsRecoverable` in `dev/doc/pgsrc.allium`.
     */
    @Test
    fun `storage-only recovery re-reads the rows the confirmed position skipped`() = runTest(timeout = 300.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val storageDir = Files.createTempDirectory("pg-recover-storage")
        val logDirs = listOf(
            Files.createTempDirectory("pg-recover-log-0"),
            Files.createTempDirectory("pg-recover-log-1"),
        )

        // Fresh primary source topic per phase so phase one's ATTACH isn't replayed — it would
        // re-attach cdc at epoch 0, and `DatabaseCatalog.attach` rejects the second attempt.
        fun attachCdc(node: Xtdb, epoch: Int) {
            node.getConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        ATTACH DATABASE cdc WITH $$
                            storage: !Local
                              path: $storageDir
                            log: !Local
                              path: ${logDirs[epoch]}
                              epoch: $epoch
                            externalSource: !Postgres
                              remote: pg
                              slotName: $slotName
                              publicationName: $pubName
                              indexer: !DirectMirror {}
                        $$""".trimIndent()
                    )
                }
            }
        }

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_recover_in (_id INT PRIMARY KEY, name TEXT)",
            "CREATE TABLE IF NOT EXISTS pg_recover_out (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_recover_in (_id, name) VALUES (1, 'snapshot-row')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_recover_in",
        )

        try {
            val (durableLsn, unflushedLsn) = openNode("test-topic-${UUID.randomUUID()}").use { node ->
                attachCdc(node, epoch = 0)
                awaitTxs(node, 2, db = "cdc")

                val cdc = (node as XtdbInternal).dbCatalog["cdc"]!!

                pgExecute("INSERT INTO pg_recover_in (_id, name) VALUES (2, 'durable-row')")
                eventually(30.seconds) {
                    assertEquals(2, xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_recover_in").size, "durable row applied")
                }

                cdc.sendFlushBlockMessage()
                eventually(30.seconds) { assertNotNull(cdc.blockCatalog.currentBlockIndex, "block persisted for cdc") }
                val durableLsn = PostgresSourceToken.parseFrom(cdc.blockCatalog.externalSourceToken!!).latestCommittedLsn

                // Writing inside the poll, not a batch up front: the received position rides on the
                // walsender's keepalives, which it sends as it works through new WAL, so a single
                // batch can leave it parked.
                var noise = 0
                eventually(60.seconds) {
                    pgExecute("INSERT INTO pg_recover_out (_id, name) VALUES (${noise++}, 'unpublished')")
                    assertTrue(
                        pgSlotConfirmedFlushLsn(slotName) > durableLsn,
                        "slot confirms past the durable block over WAL holding nothing of ours",
                    )
                }

                pgExecute("INSERT INTO pg_recover_in (_id, name) VALUES (3, 'unflushed-row')")
                eventually(30.seconds) {
                    assertEquals(
                        listOf("unflushed-row"),
                        xtQueryDb(node, "cdc", "SELECT name FROM public.pg_recover_in WHERE _id = 3").map { it["name"] },
                        "unflushed row is queryable before the restart",
                    )
                }

                assertEquals(
                    durableLsn,
                    PostgresSourceToken.parseFrom(cdc.blockCatalog.externalSourceToken!!).latestCommittedLsn,
                    "test precondition: no block covers the unflushed row, so the replica log and the live index are its only copies",
                )

                val appliedLsn = latestPostgresToken(node).latestCommittedLsn
                assertTrue(
                    appliedLsn > pgSlotConfirmedFlushLsn(slotName),
                    "test precondition: the unflushed row sits above the confirmed position, so Postgres still holds it",
                )
                durableLsn to appliedLsn
            }

            // At the same epoch `validateOffsets` rejects a log whose latest offset sits below the
            // block's watermark and the node won't start — /ops/backup-and-restore/out-of-sync-log.
            openNode("test-topic-${UUID.randomUUID()}").use { node ->
                attachCdc(node, epoch = 1)

                assertEquals(
                    durableLsn,
                    PostgresSourceToken.parseFrom(
                        (node as XtdbInternal).dbCatalog["cdc"]!!.blockCatalog.externalSourceToken!!
                    ).latestCommittedLsn,
                    "resumed from the block cut before the restart — a block flushed on shutdown would make the unflushed row durable and void everything below",
                )

                eventually(60.seconds) {
                    assertEquals(
                        listOf(1, 2, 3),
                        xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_recover_in ORDER BY _id")
                            .map { (it["_id"] as Number).toInt() },
                        "the row that was never blocked is re-read from the upstream",
                    )
                }

                assertEquals(
                    listOf("durable-row"),
                    xtQueryDb(node, "cdc", "SELECT name FROM public.pg_recover_in FOR ALL VALID_TIME WHERE _id = 2")
                        .map { it["name"] },
                    "resumed rather than re-snapshotted — three rows alone wouldn't tell them apart, a re-snapshot would write a second version of the blocked row",
                )

                assertEquals(
                    unflushedLsn, latestPostgresToken(node).latestCommittedLsn,
                    "resumed to the unflushed row and no further",
                )

                assertPrimaryDbHealthy(node)
            }
        } finally {
            (logDirs + storageDir).forEach { it.toFile().deleteRecursively() }
        }
    }

    /**
     * Records how we behave when a replication slot is dropped and recreated under a running source.
     * This is **not supported** and users MUST NOT rely on it — it's written down so that we know what
     * happens, not because we're committing to it continuing to happen.
     *
     * Postgres does not error on a resume LSN that predates the slot's `confirmed_flush_lsn` — it
     * silently forwards to the slot's own position, logging `"%X/%X has been already streamed,
     * forwarding to %X/%X"` at LOG level server-side (`CreateDecodingContext`, logical.c). Everything
     * between our token and the new slot's consistent point is skipped with no client-visible signal,
     * and the resume path never re-snapshots to reconcile it (`snapshotCompleted = true` goes straight
     * to `streamChanges`). A dropped DELETE is the worst of it: the row stays in XT looking like live
     * data, so the divergence doesn't even read as missing data.
     *
     * Reaching this state takes deliberate operator action — XTDB itself never drops or recreates a
     * slot (`openSnapshot` only ever creates one that doesn't already exist, and fails if it does).
     *
     * Disabled rather than deleted: as a live test it asserts data loss, so it would fail the day we
     * decide to guard against this — for the right reasons. Enable it by hand to check the behaviour
     * hasn't drifted.
     */
    @Disabled("Characterises unsupported behaviour: a slot recreated under a running source. A record of how we behave, not a guarantee.")
    @Test
    fun `slot recreation silently drops changes`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_slot_gap (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_slot_gap (_id, name) VALUES (1, 'snapshot-row')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_slot_gap",
        )

        val tokenLsn = openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            // 1 table batch + 1 completion marker = 2 txs
            awaitTxs(node, 2, db = "cdc")

            // stream a row so the token advances past the snapshot's consistent point
            pgExecute("INSERT INTO pg_slot_gap (_id, name) VALUES (2, 'streamed-row')")
            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_slot_gap WHERE _id = 2").isNotEmpty(), "streamed row appears")
            }

            val token = latestPostgresToken(node)
            assertTrue(token.snapshotCompleted, "test precondition: restart should take the resume path")
            token.latestCommittedLsn
        }

        eventually(10.seconds) { assertTrue(pgSlotState(slotName) == false, "slot released after node close") }

        // the gap — one of each op type, with the node down
        pgExecute(
            "INSERT INTO pg_slot_gap (_id, name) VALUES (3, 'gap-insert')",
            "UPDATE pg_slot_gap SET name = 'gap-update' WHERE _id = 2",
            "DELETE FROM pg_slot_gap WHERE _id = 1",
        )

        // an operator reprovisions the slot — same name, fresh consistent point
        pgExecute(
            "SELECT pg_drop_replication_slot('$slotName')",
            "SELECT pg_create_logical_replication_slot('$slotName', 'pgoutput')",
        )

        // written past the new slot's consistent point, so this one does arrive — the fence that
        // proves the source resumed rather than merely lagging
        pgExecute("INSERT INTO pg_slot_gap (_id, name) VALUES (4, 'post-recreate')")

        val slotLsn = pgSlotConfirmedFlushLsn(slotName)
        assertTrue(
            slotLsn > tokenLsn,
            "test precondition: recreated slot ($slotLsn) should be ahead of our token ($tokenLsn)"
        )

        openNode(sourceTopic).use { node ->
            eventually(60.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_slot_gap WHERE _id = 4").isNotEmpty(), "post-recreate row appears")
            }

            // pgoutput delivers in LSN order, so the fence having landed means everything in the
            // gap was skipped outright — not still in flight
            val rows = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.pg_slot_gap ORDER BY _id")

            assertEquals(listOf(1, 2, 4), rows.map { (it["_id"] as Number).toInt() }, "gap insert (3) is dropped")
            assertEquals("streamed-row", rows[1]["name"], "gap update is dropped — XT keeps the stale value")
            assertEquals("snapshot-row", rows[0]["name"], "gap delete is dropped — row 1 lives on in XT")

            // Postgres has 2 (updated), 3, 4 — XT has 1 (should be gone), 2 (stale), 4
            assertEquals(
                listOf(2, 3, 4), pgQueryIds("SELECT _id FROM pg_slot_gap ORDER BY _id"),
                "meanwhile Postgres has moved on"
            )

            // no error, no stall — the node is happily wrong
            assertPrimaryDbHealthy(node)
        }
    }

    @Test
    fun `restart succeeds when txId has diverged from the source-log offset`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val storageDir = Files.createTempDirectory("pg-cdc-storage")

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_diverge (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_diverge (_id, name) VALUES (1, 'row-1')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_diverge",
        )

        try {
            // Phase 1: ingest enough CDC events that the indexer's txId climbs well past
            // the secondary's source-log offset. Each committed CDC event bumps txId, but
            // the source log only carries control messages (attach, flush, block-uploaded) —
            // so the two counters diverge under entirely normal operation.
            openNode(sourceTopic).use { node ->
                attachPostgresSourceWithLocalStorage(node, storageDir, slotName = slotName, publicationName = pubName)

                // snapshot: 1 table batch + 1 completion marker = 2 txs
                awaitTxs(node, 2, db = "cdc")

                for (i in 2..21) pgExecute("INSERT INTO pg_diverge (_id, name) VALUES ($i, 'row-$i')")
                eventually(60.seconds) {
                    assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_diverge").size == 21, "all streamed rows ingested")
                }

                // Flush a block so the cdc block catalog persists latestCompletedTx (high txId)
                // and latestProcessedMsgId (low source-log offset) to durable storage.
                val cdc = (node as XtdbInternal).dbCatalog["cdc"]!!
                cdc.sendFlushBlockMessage()
                eventually(30.seconds) {
                    assertTrue(cdc.blockCatalog.currentBlockIndex != null, "block persisted for cdc")
                }

                val txId = cdc.blockCatalog.latestCompletedTx!!.txId
                val sourceOffset = cdc.sourceLog.latestSubmittedOffset()
                assertTrue(
                    txId > sourceOffset,
                    "test precondition: txId ($txId) should exceed source-log offset ($sourceOffset)"
                )
            }

            // Phase 2: restart against the same source topic + persisted storage.
            // Pre-31b825623 openNode would throw IllegalStateException (due to us validating offsets against txId)
            // and the node would fail to start.
            openNode(sourceTopic).use { node ->
                eventually(60.seconds) {
                    assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_diverge").size == 21, "cdc db queryable after restart")
                }

                assertPrimaryDbHealthy(node)
            }
        } finally {
            storageDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `read-only replica of a CDC database survives a block flush`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        // a shared Kafka log and local storage are what let 'ro' follow 'cdc' from one node
        val cdcTopic = "cdc-log-${UUID.randomUUID()}"
        val storageDir = Files.createTempDirectory("pg-cdc-ro-storage")

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_ro (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_ro (_id, name) VALUES (1, 'Alice')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_ro",
        )

        try {
            openNode(sourceTopic).use { node ->
                attachPostgresSourceWithLocalStorage(
                    node, storageDir,
                    logTopic = cdcTopic, slotName = slotName, publicationName = pubName,
                )

                // 1 table batch + 1 completion marker = 2 txs
                awaitTxs(node, 2, db = "cdc")

                attachReadOnlyReplica(node, "ro", cdcTopic, storageDir)

                val dbCatalog = (node as XtdbInternal).dbCatalog
                val cdc = dbCatalog["cdc"]!!
                val ro = dbCatalog["ro"]!!

                eventually(30.seconds) {
                    assertTrue(xtQueryDb(node, "ro", "SELECT _id FROM public.pg_ro WHERE _id = 1").isNotEmpty(),
                        "ro replays the CDC snapshot")
                }

                cdc.sendFlushBlockMessage()
                eventually(30.seconds) {
                    assertNotNull(cdc.blockCatalog.currentBlockIndex, "block persisted for cdc")
                }

                // the boundary carries cdc's source-log watermark, which stays low while the CDC
                // txIds climb — 'ro' only survives it by tracking that watermark itself (#5680)
                pgExecute("INSERT INTO pg_ro (_id, name) VALUES (2, 'Bob')")

                // ingestionError first: `eventually` reports the *last* failure, so a regression
                // names the IngestionStoppedException rather than timing out anonymously
                eventually(30.seconds) {
                    assertNull(ro.ingestionError, "ro ingestion must survive cdc's block flush")
                    assertTrue(xtQueryDb(node, "ro", "SELECT _id FROM public.pg_ro WHERE _id = 2").isNotEmpty(),
                        "ro follows cdc past the block boundary")
                }

                assertEquals(
                    setOf(1, 2),
                    xtQueryDb(node, "ro", "SELECT _id FROM public.pg_ro").map { it["_id"] }.toSet(),
                    "ro sees both the pre-flush snapshot row and the post-flush streamed row",
                )
            }
        } finally {
            storageDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `metrics populate against real Postgres`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_metrics (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_metrics (_id, name) VALUES (1, 'snapshot-row')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_metrics",
        )

        openNode(sourceTopic).use { node ->
            // Reach into the node's primary registry directly — `addMeterRegistry` assumes a
            // composite registry, which the node doesn't currently use (see Metrics.kt).
            val metrics = (node as ILookup).valAt(Keyword.intern(null, "metrics-registry")) as MeterRegistry

            attachPostgresSource(node, slotName = slotName, publicationName = pubName)
            awaitTxs(node, 2, db = "cdc")

            pgExecute("INSERT INTO pg_metrics (_id, name) VALUES (2, 'stream-row')")
            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_metrics WHERE _id = 2").isNotEmpty(), "streamed row visible")
            }

            fun metric(name: String) =
                metrics.find(name).tags("db", "cdc", "source", slotName, "source_type", "postgres")

            // The counters are incremented in `promoteDurable`, which the poll loop reaches only
            // after `settle` has completed the submit handle — and `settle` makes the row queryable
            // before it does that. So the row being visible above doesn't imply it's been counted.
            eventually(30.seconds) {
                assertTrue((metric("xtdb.postgres_source.events.total").counter()?.count() ?: 0.0) >= 1.0, "events.total counted the streamed row")
            }
            eventually(30.seconds) {
                assertTrue((metric("xtdb.postgres_source.commits.total").counter()?.count() ?: 0.0) >= 1.0, "commits.total counted the streamed commit")
            }

            // Confirms the pg_replication_slots query path works against real Postgres —
            // the gauge pulls on read so this should be populated immediately after streaming.
            assertTrue((metric("xtdb.postgres_source.wal_lag_bytes").gauge()?.value() ?: -1.0) >= 0.0,
                "wal_lag_bytes should be readable from pg_replication_slots once streaming")
        }
    }

    @Test
    fun `partial snapshot failure when slot already exists`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_partial (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_partial (_id, name) VALUES (1, 'Alice')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_partial",
        )

        // Pre-create the replication slot — simulates a previous attempt
        // that created the slot but crashed before completing the snapshot.
        pgExecute("SELECT pg_create_logical_replication_slot('$slotName', 'pgoutput')")

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            // The CDC source will try to CREATE_REPLICATION_SLOT with the same name
            // and fail because it already exists. Wait long enough for the attempt.
            runInterruptible { Thread.sleep(10_000) }

            // Snapshot should have failed — no data ingested.
            val result = runCatching {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_partial")
            }

            assertTrue(
                result.isFailure || result.getOrNull().isNullOrEmpty(),
                "Snapshot should fail when slot already exists — expected error or no data, got: ${result.getOrNull()}"
            )

            assertPrimaryDbHealthy(node)
        }
    }

    @Test
    fun `source fails when postgres dies during snapshot`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val dedicatedPg = newDedicatedPostgres()
        Startables.deepStart(dedicatedPg).join()

        try {
            dedicatedPg.executeSql(
                "CREATE TABLE pg_snap_kill (_id INT PRIMARY KEY, name TEXT)",
                // Enough rows that the snapshot makes many round trips, leaving a
                // wide window to kill the container mid-stream.
                "INSERT INTO pg_snap_kill SELECT g, 'row-' || g FROM generate_series(1, 50000) g",
                "CREATE PUBLICATION $pubName FOR TABLE pg_snap_kill",
            )

            openNode(sourceTopic, pgContainer = dedicatedPg).use { node ->
                attachPostgresSource(node, slotName = slotName, publicationName = pubName)

                // Wait until the first batch lands — proves the snapshot has started.
                eventually(30.seconds) {
                    assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_snap_kill LIMIT 1").isNotEmpty(), "snapshot in flight")
                }

                dedicatedPg.stop()

                val cdc = (node as XtdbInternal).dbCatalog
                eventually(30.seconds) {
                    assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError when PG dies during snapshot")
                }
                assertNotNull(cdc["cdc"]?.ingestionError,
                    "snapshot failure must surface IngestionStoppedException, not silently exit")

                assertFalse(latestPostgresToken(node).snapshotCompleted,
                    "must catch PG mid-snapshot; snapshot completed before kill")

                assertPrimaryDbHealthy(node)
            }
        } finally {
            runCatching { dedicatedPg.stop() }
        }
    }

    @Test
    fun `source surfaces ingestion error when postgres connection is lost mid-stream`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val dedicatedPg = newDedicatedPostgres()
        Startables.deepStart(dedicatedPg).join()

        try {
            dedicatedPg.executeSql(
                "CREATE TABLE pg_stream_kill (_id INT PRIMARY KEY, name TEXT)",
                "INSERT INTO pg_stream_kill (_id, name) VALUES (1, 'Alice')",
                "CREATE PUBLICATION $pubName FOR TABLE pg_stream_kill",
            )

            openNode(sourceTopic, pgContainer = dedicatedPg).use { node ->
                attachPostgresSource(node, slotName = slotName, publicationName = pubName)

                // Snapshot completes, then a streamed insert proves the replication
                // stream is live before we kill the upstream.
                awaitTxs(node, 2, db = "cdc")
                dedicatedPg.executeSql("INSERT INTO pg_stream_kill (_id, name) VALUES (2, 'Bob')")
                eventually(30.seconds) {
                    assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_stream_kill WHERE _id = 2").isNotEmpty(), "streamed row visible")
                }

                dedicatedPg.stop()

                val cdc = (node as XtdbInternal).dbCatalog
                eventually(60.seconds) {
                    assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError when PG dies mid-stream")
                }
                assertNotNull(cdc["cdc"]?.ingestionError,
                    "stream failure must surface IngestionStoppedException, not silently exit")

                assertPrimaryDbHealthy(node)
            }
        } finally {
            runCatching { dedicatedPg.stop() }
        }
    }

    @Test
    fun `slots are created with failover enabled`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_failover (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_failover (_id, name) VALUES (1, 'Alice')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_failover",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            awaitTxs(node, 2, db = "cdc")

            assertEquals(true, pgSlotFailover(slotName), "slot created with failover enabled")
            assertEquals(setOf("Alice"),
                xtQueryDb(node, "cdc", "SELECT name FROM public.pg_failover").map { it["name"] }.toSet(),
                "a failover slot snapshots and streams like any other")
        }
    }

    @Test
    fun `postgres before 17 is unsupported, and says so`() = runTest(timeout = 180.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val pg16 = newDedicatedPostgres("postgres:16-alpine")
        Startables.deepStart(pg16).join()

        try {
            pg16.executeSql(
                "CREATE TABLE pg_old (_id INT PRIMARY KEY, name TEXT)",
                "CREATE PUBLICATION $pubName FOR TABLE pg_old",
            )

            openNode(sourceTopic, pgContainer = pg16).use { node ->
                attachPostgresSource(node, slotName = slotName, publicationName = pubName)

                val cdc = (node as XtdbInternal).dbCatalog
                eventually(30.seconds) {
                    assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError on a pre-17 server")
                }

                val chain = generateSequence(cdc["cdc"]?.ingestionError as Throwable?) { it.cause }.toList()
                assertTrue(
                    chain.any { it.message?.contains("needs PostgreSQL 17 or later") == true },
                    "we name the requirement, since Postgres doesn't: " +
                            chain.joinToString { "${it::class.simpleName}: ${it.message}" },
                )
                assertTrue(
                    chain.any { it.message?.contains("unrecognized option: failover") == true },
                    "and Postgres' own message survives: " +
                            chain.joinToString { "${it::class.simpleName}: ${it.message}" },
                )

                assertPrimaryDbHealthy(node)
            }
        } finally {
            runCatching { pg16.stop() }
        }
    }

    @Test
    fun `source fails when publication does not exist`() = runTest(timeout = 60.seconds) {
        // No CREATE PUBLICATION — pgoutput silently emits no events when the publication
        // is missing, so without an explicit check the source would create the slot and
        // "stream" forever yielding nothing. The source MUST surface ingestionError instead.
        val pubName = "missing_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_missing_pub (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_missing_pub (_id, name) VALUES (1, 'Alice')",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            val cdc = (node as XtdbInternal).dbCatalog
            eventually(30.seconds) {
                assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError for missing publication")
            }
            assertNotNull(cdc["cdc"]?.ingestionError,
                "missing publication must surface IngestionStoppedException, not silently stall")

            assertPrimaryDbHealthy(node)
        }
    }

    @Test
    fun `source fails when the replication slot already exists`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_dup_slot (_id INT PRIMARY KEY, name TEXT)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_dup_slot",
            "SELECT pg_create_logical_replication_slot('$slotName', 'pgoutput')",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            val cdc = (node as XtdbInternal).dbCatalog
            eventually(30.seconds) {
                assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError for a pre-existing slot")
            }

            val chain = generateSequence(cdc["cdc"]?.ingestionError as Throwable?) { it.cause }.toList()
            val conflict = chain.filterIsInstance<Conflict>().firstOrNull()
                ?: throw AssertionError(
                    "a pre-existing slot must surface as Conflict, got: " +
                            chain.joinToString { "${it::class.simpleName}: ${it.message}" }
                )

            assertEquals(
                Keyword.intern("xtdb.postgres", "slot-exists"),
                conflict.data.valAt(Keyword.intern("xtdb.error", "code")),
            )
            assertTrue(
                conflict.message?.contains("already exists") == true,
                "Postgres' own message survives the wrap, got: ${conflict.message}",
            )

            assertPrimaryDbHealthy(node)
        }
    }

    @Test
    fun `source fails when postgres credentials are wrong`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_bad_creds (_id INT PRIMARY KEY, name TEXT)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_bad_creds",
        )

        openNode(sourceTopic, pgPassword = "wrong-password").use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            val cdc = (node as XtdbInternal).dbCatalog
            eventually(30.seconds) {
                assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError on auth failure")
            }
            assertNotNull(cdc["cdc"]?.ingestionError,
                "auth failure must surface IngestionStoppedException")

            assertPrimaryDbHealthy(node)
        }
    }

    @Test
    fun `multi-table snapshot and streaming`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_mt_users (_id INT PRIMARY KEY, name TEXT)",
            "CREATE TABLE IF NOT EXISTS pg_mt_orders (_id INT PRIMARY KEY, user_id INT, amount NUMERIC)",
            "INSERT INTO pg_mt_users (_id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO pg_mt_orders (_id, user_id, amount) VALUES (1, 1, 99.99)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_mt_users, pg_mt_orders",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            // Wait for both tables to be snapshotted
            eventually(30.seconds) {
                assertTrue(
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_users").size == 2 &&
                        xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_orders").size == 1,
                    "both tables snapshotted"
                )
            }

            val users = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.pg_mt_users ORDER BY _id")
            assertEquals("Alice", users[0]["name"])
            assertEquals("Bob", users[1]["name"])

            val orders = xtQueryDb(node, "cdc", "SELECT _id, user_id, amount FROM public.pg_mt_orders")
            assertEquals(1, orders.size)

            // Stream changes to both tables in a single PG transaction
            pgExecute(
                "BEGIN",
                "INSERT INTO pg_mt_users (_id, name) VALUES (3, 'Charlie')",
                "INSERT INTO pg_mt_orders (_id, user_id, amount) VALUES (2, 3, 42.00)",
                "COMMIT",
            )

            eventually(30.seconds) {
                assertTrue(
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_users WHERE _id = 3").isNotEmpty() &&
                        xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_orders WHERE _id = 2").isNotEmpty(),
                    "streaming changes to both tables"
                )
            }
        }
    }

    @Test
    fun `tables outside the publication are not replicated`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_pub_in (_id INT PRIMARY KEY, name TEXT)",
            "CREATE TABLE IF NOT EXISTS pg_pub_out (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_pub_in (_id, name) VALUES (1, 'in-snap')",
            "INSERT INTO pg_pub_out (_id, name) VALUES (1, 'out-snap')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_pub_in",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_pub_in WHERE _id = 1").isNotEmpty(), "included table snapshotted")
            }

            // Stream a change to both tables; only the included one should land.
            pgExecute(
                "INSERT INTO pg_pub_in (_id, name) VALUES (2, 'in-stream')",
                "INSERT INTO pg_pub_out (_id, name) VALUES (2, 'out-stream')",
            )

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_pub_in WHERE _id = 2").isNotEmpty(), "streaming change to included table")
            }

            // pg_pub_out is not in the publication — neither its snapshot row
            // nor its streamed insert should ever appear, so the table is never
            // materialised in XTDB at all.
            val publicTables = xtQueryDb(
                node, "cdc",
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                """.trimIndent(),
            ).map { it["table_name"] as String }
            assertTrue(publicTables.contains("pg_pub_in"), "expected pg_pub_in in $publicTables")
            assertTrue(!publicTables.contains("pg_pub_out"), "pg_pub_out should not be replicated, got $publicTables")
        }
    }

    @Test
    fun `schema evolution - add column during streaming`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_evolve (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_evolve (_id, name) VALUES (1, 'Alice')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_evolve",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_evolve WHERE _id = 1").isNotEmpty(), "Alice snapshotted")
            }

            // ALTER TABLE in PG while streaming is active
            pgExecute(
                "ALTER TABLE pg_evolve ADD COLUMN email TEXT",
                "INSERT INTO pg_evolve (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
            )

            eventually(30.seconds) {
                val rows = xtQueryDb(node, "cdc", "SELECT _id, email FROM public.pg_evolve WHERE _id = 2")
                assertEquals("bob@example.com", rows.firstOrNull()?.get("email"), "Bob with new column appears")
            }

            // Alice (pre-evolution row) should have null for the new column
            val alice = xtQueryDb(node, "cdc", "SELECT _id, name, email FROM public.pg_evolve WHERE _id = 1")
            assertEquals(1, alice.size)
            assertEquals(null, alice[0]["email"])
        }
    }

    @Test
    fun `snapshot and streaming CDC lifecycle`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_cdc_users (_id INT PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO pg_cdc_users (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_cdc_users",
        )

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, publicationName = pubName)

            // Phase 1: snapshot
            // 1 tx per table + 1 completion marker = 2 txs
            awaitTxs(node, 2, db = "cdc")

            val snapshotRows = xtQueryDb(
                    node, "cdc",
                "SELECT _id, name, email FROM public.pg_cdc_users ORDER BY _id"
            )
            assertEquals(1, snapshotRows.size, "Snapshot should ingest Alice")
            assertEquals("alice@example.com", snapshotRows[0]["email"])

            // Phase 2: streaming
            pgExecute(
                "INSERT INTO pg_cdc_users (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                "UPDATE pg_cdc_users SET email = 'alice-new@example.com' WHERE _id = 1",
            )

            eventually(30.seconds) {
                assertTrue(
                    xtQueryDb(node, "cdc", "SELECT email FROM public.pg_cdc_users WHERE _id = 1")
                        .firstOrNull()?.get("email") == "alice-new@example.com",
                    "Alice updated"
                )
            }

            val bob = xtQueryDb(node, "cdc", "SELECT email FROM public.pg_cdc_users WHERE _id = 2")
            assertEquals(1, bob.size)
            assertEquals("bob@example.com", bob[0]["email"])

            // Phase 3: delete
            pgExecute("DELETE FROM pg_cdc_users WHERE _id = 2")

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_cdc_users WHERE _id = 2").isEmpty(), "Bob deleted")
            }
        }
    }

    @Test
    fun `pg types ingest correctly across snapshot and streaming`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            """CREATE TABLE IF NOT EXISTS pg_types (
                  _id INT PRIMARY KEY,
                  span INTERVAL,
                  dt DATE,
                  tm TIME,
                  ts TIMESTAMP,
                  tstz TIMESTAMPTZ,
                  meta JSONB,
                  uid UUID,
                  amount NUMERIC,
                  enabled BOOLEAN,
                  note TEXT
               )""",
            """INSERT INTO pg_types VALUES (
                  1,
                  INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds',
                  DATE '2024-03-15',
                  TIME '12:34:56.789',
                  TIMESTAMP '2024-03-15 12:34:56.789',
                  TIMESTAMPTZ '2024-03-15 12:34:56.789+02',
                  '{"foo": "bar", "n": 42}'::JSONB,
                  '11111111-2222-3333-4444-555555555555'::UUID,
                  123.456,
                  TRUE,
                  'snapshot row'
               )""",
            "CREATE PUBLICATION $pubName FOR TABLE pg_types",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, publicationName = pubName)

            val cols = "span, dt, tm, ts, tstz, meta, uid, amount, enabled, note"

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_types WHERE _id = 1").isNotEmpty(), "snapshot row ingested")
            }
            val snap = xtQueryDb(node, "cdc", "SELECT $cols FROM public.pg_types WHERE _id = 1").single()
            assertTypedRow(
                snap,
                expectedSpan = Interval(14, 3, 4L * 3_600_000_000_000L + 5L * 60_000_000_000L + 6_789_000_000L),
                expectedDate = LocalDate.of(2024, 3, 15),
                expectedTime = LocalTime.of(12, 34, 56, 789_000_000),
                expectedTs = LocalDateTime.of(2024, 3, 15, 12, 34, 56, 789_000_000),
                expectedTstzInstant = ZonedDateTime.of(2024, 3, 15, 12, 34, 56, 789_000_000, ZoneOffset.ofHours(2)).toInstant(),
                expectedMetaFoo = "bar",
                expectedUid = UUID.fromString("11111111-2222-3333-4444-555555555555"),
                expectedAmount = BigDecimal("123.456"),
                expectedEnabled =true,
                expectedNote = "snapshot row",
            )

            // Streaming path
            pgExecute(
                """INSERT INTO pg_types VALUES (
                      2,
                      INTERVAL '-1 day 30 minutes',
                      DATE '2025-01-31',
                      TIME '00:00:01',
                      TIMESTAMP '2025-01-31 23:59:58',
                      TIMESTAMPTZ '2025-01-31 23:59:58-05',
                      '{"foo": "qux"}'::JSONB,
                      '22222222-3333-4444-5555-666666666666'::UUID,
                      -0.001,
                      FALSE,
                      'streamed row'
                   )"""
            )

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_types WHERE _id = 2").isNotEmpty(), "streaming row ingested")
            }
            val streamed = xtQueryDb(node, "cdc", "SELECT $cols FROM public.pg_types WHERE _id = 2").single()
            assertTypedRow(
                streamed,
                expectedSpan = Interval(0, -1, 30L * 60_000_000_000L),
                expectedDate = LocalDate.of(2025, 1, 31),
                expectedTime = LocalTime.of(0, 0, 1),
                expectedTs = LocalDateTime.of(2025, 1, 31, 23, 59, 58),
                expectedTstzInstant = ZonedDateTime.of(2025, 1, 31, 23, 59, 58, 0, ZoneOffset.ofHours(-5)).toInstant(),
                expectedMetaFoo = "qux",
                expectedUid = UUID.fromString("22222222-3333-4444-5555-666666666666"),
                expectedAmount = BigDecimal("-0.001"),
                expectedEnabled =false,
                expectedNote = "streamed row",
            )
        }
    }

    @Test
    fun `user-defined enum columns replicate across snapshot and streaming`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        // A user-defined type makes pgoutput emit a Type ('Y') message ahead of the Relation that
        // references it; the source loop used to die on that message. Enums are the common case.
        pgExecute(
            "DROP TABLE IF EXISTS pg_moods",
            "DROP TYPE IF EXISTS mood",
            "CREATE TYPE mood AS ENUM ('happy', 'sad', 'meh')",
            "CREATE TABLE pg_moods (_id INT PRIMARY KEY, feeling mood, note TEXT)",
            "INSERT INTO pg_moods VALUES (1, 'happy', 'snapshot row')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_moods",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, publicationName = pubName)

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_moods WHERE _id = 1").isNotEmpty(), "snapshot enum row ingested")
            }
            assertEquals(
                "happy",
                xtQueryDb(node, "cdc", "SELECT feeling FROM public.pg_moods WHERE _id = 1").single()["feeling"]
            )

            pgExecute("INSERT INTO pg_moods VALUES (2, 'sad', 'streamed row')")

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_moods WHERE _id = 2").isNotEmpty(), "streaming enum row ingested")
            }
            assertEquals(
                "sad",
                xtQueryDb(node, "cdc", "SELECT feeling FROM public.pg_moods WHERE _id = 2").single()["feeling"]
            )
        }
    }

    private fun assertTypedRow(
        row: Map<String, Any?>,
        expectedSpan: Interval,
        expectedDate: LocalDate,
        expectedTime: LocalTime,
        expectedTs: LocalDateTime,
        expectedTstzInstant: java.time.Instant,
        expectedMetaFoo: String,
        expectedUid: UUID,
        expectedAmount: BigDecimal,
        expectedEnabled: Boolean,
        expectedNote: String,
    ) {
        // assertAll runs every check and reports all failures together,
        // rather than stopping at the first.
        Assertions.assertAll(
            { assertEquals(expectedSpan, row["span"]) },
            { assertEquals(expectedDate, row["dt"]) },
            { assertEquals(expectedTime, row["tm"]) },
            { assertEquals(expectedTs, row["ts"]) },
            { assertEquals(expectedTstzInstant, (row["tstz"] as ZonedDateTime).toInstant()) },
            {
                @Suppress("UNCHECKED_CAST") val meta = row["meta"] as Map<String, Any?>
                assertEquals(expectedMetaFoo, meta["foo"])
            },
            { assertEquals(expectedUid, row["uid"]) },
            { assertEquals(0, expectedAmount.compareTo(row["amount"] as BigDecimal)) },
            { assertEquals(expectedEnabled, row["enabled"]) },
            { assertEquals(expectedNote, row["note"]) },
        )
    }

    @Test
    fun `cancellation releases replication slot`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_cancel (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO pg_cancel (_id, name) VALUES (1, 'Alice')",
            "CREATE PUBLICATION $pubName FOR TABLE pg_cancel",
        )

        // Start a node, let it snapshot and begin streaming (holds the slot)
        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            awaitTxs(node, 2, db = "cdc")

            // Stream an insert so we know streaming is running and holding the slot
            pgExecute("INSERT INTO pg_cancel (_id, name) VALUES (2, 'Bob')")
            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_cancel WHERE _id = 2").isNotEmpty(), "Bob appears")
            }

            // Streaming connection is now holding the slot
            assertEquals(true, pgSlotState(slotName), "Slot should be active while streaming")
        }
        // node.close() triggers LeaderLogProcessor.close() → extJob.cancel()
        // The closeOnCancel child coroutine should force-close the PG connection,
        // releasing the replication slot.

        eventually(10.seconds) {
            assertTrue(pgSlotState(slotName) == false, "slot released after node close")
        }
    }

    @Test
    fun `successive updates are each preserved in history`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_collapse (_id INT PRIMARY KEY, name TEXT)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_collapse",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            pgExecute("INSERT INTO pg_collapse (_id, name) VALUES (1, 'v0')")
            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_collapse WHERE _id = 1").isNotEmpty(), "v0 ingested")
            }

            pgExecute("BEGIN", "UPDATE pg_collapse SET name = 'v1' WHERE _id = 1", "COMMIT")
            pgExecute("BEGIN", "UPDATE pg_collapse SET name = 'v2' WHERE _id = 1", "COMMIT")
            pgExecute("BEGIN", "UPDATE pg_collapse SET name = 'v3' WHERE _id = 1", "COMMIT")

            eventually(30.seconds) {
                assertTrue(
                    xtQueryDb(node, "cdc", "SELECT name FROM public.pg_collapse WHERE _id = 1")
                        .firstOrNull()?.get("name") == "v3",
                    "final update applied"
                )
            }

            val history = xtQueryDb(node, "cdc", "SELECT name FROM public.pg_collapse FOR ALL VALID_TIME WHERE _id = 1 ORDER BY _valid_from")
                .map { it["name"] as String }

            assertEquals(listOf("v0", "v1", "v2", "v3"), history)
        }
    }

    @Test
    fun `explicit _valid_from on a CDC row sets the version valid-time`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        // empty at attach time, so the row streams through writeOp (where _valid_from is handled)
        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_vf (_id INT PRIMARY KEY, name TEXT, _valid_from TIMESTAMPTZ)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_vf",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            pgExecute("INSERT INTO pg_vf (_id, name, _valid_from) VALUES (1, 'Alice', TIMESTAMPTZ '2020-01-01 00:00:00+00')")

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_vf WHERE _id = 1").isNotEmpty(), "row ingested")
            }

            val document = xtQueryDb(node, "cdc", "SELECT *, _valid_from FROM public.pg_vf WHERE _id = 1").single()
            assertEquals("Alice", document["name"])

            val validFrom = (document["_valid_from"] as ZonedDateTime).toInstant()
            assertEquals(java.time.Instant.parse("2020-01-01T00:00:00Z"), validFrom)
        }
    }

    @Test
    fun `explicit _valid_from and _valid_to on a CDC row sets the version bounds`() = runTest(timeout = 60.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_vt (_id INT PRIMARY KEY, name TEXT, _valid_from TIMESTAMPTZ, _valid_to TIMESTAMPTZ)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_vt",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            pgExecute(
                """INSERT INTO pg_vt (_id, name, _valid_from, _valid_to)
                   VALUES (1, 'Alice',
                           TIMESTAMPTZ '2020-01-01 00:00:00+00',
                           TIMESTAMPTZ '2021-01-01 00:00:00+00')"""
            )

            eventually(30.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_vt FOR ALL VALID_TIME WHERE _id = 1").isNotEmpty(), "row ingested")
            }

            val document = xtQueryDb(node, "cdc",
                "SELECT *, _valid_from, _valid_to FROM public.pg_vt FOR ALL VALID_TIME WHERE _id = 1").single()
            assertEquals("Alice", document["name"])

            val validFrom = (document["_valid_from"] as ZonedDateTime).toInstant()
            val validTo = (document["_valid_to"] as ZonedDateTime).toInstant()
            assertEquals(java.time.Instant.parse("2020-01-01T00:00:00Z"), validFrom)
            assertEquals(java.time.Instant.parse("2021-01-01T00:00:00Z"), validTo)
        }
    }

    @Test
    fun `non-TIMESTAMPTZ _valid_from fails the source loudly`() = runTest(timeout = 60.seconds) {
        // Anything other than TIMESTAMPTZ is ambiguous (no session zone in CDC) — surface
        // an ingestionError rather than silently falling back to system time.
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE IF NOT EXISTS pg_vf_bad (_id INT PRIMARY KEY, name TEXT, _valid_from TIMESTAMP)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_vf_bad",
        )

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            pgExecute("INSERT INTO pg_vf_bad (_id, name, _valid_from) VALUES (1, 'Alice', TIMESTAMP '2020-01-01 00:00:00')")

            val cdc = (node as XtdbInternal).dbCatalog
            eventually(30.seconds) {
                assertTrue(cdc["cdc"]?.ingestionError != null, "cdc surfaces ingestionError for non-TIMESTAMPTZ _valid_from")
            }

            val cause = generateSequence(cdc["cdc"]?.ingestionError as Throwable?) { it.cause }.toList()
            assertTrue(cause.any { it.message?.contains("'_valid_from' must be a TIMESTAMPTZ") == true },
                "expected loud TIMESTAMPTZ-required error, got: ${cause.map { "${it::class.simpleName}: ${it.message}" }}")

            assertPrimaryDbHealthy(node)
        }
    }

    /**
     * Returns true if the slot is active, false if it exists but is inactive, null if the slot doesn't exist.
     */
    private fun pgSlotState(slotName: String): Boolean? {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT active FROM pg_replication_slots WHERE slot_name = '$slotName'").use { rs ->
                    return if (rs.next()) rs.getBoolean("active") else null
                }
            }
        }
    }

    private fun pgSlotFailover(slotName: String): Boolean? =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT failover FROM pg_replication_slots WHERE slot_name = '$slotName'").use { rs ->
                    if (rs.next()) rs.getBoolean("failover") else null
                }
            }
        }

    private fun pgSlotConfirmedFlushLsn(slotName: String): Long =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT confirmed_flush_lsn::text AS lsn FROM pg_replication_slots WHERE slot_name = '$slotName'")
                    .use { rs ->
                        assertTrue(rs.next(), "slot '$slotName' should exist")
                        LogSequenceNumber.valueOf(rs.getString("lsn")).asLong()
                    }
            }
        }

    private fun pgQueryIds(sql: String): List<Int> =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getInt(1)) } }
            }
        }

    @Test
    fun `streamed tx system-time is the postgres commit time`() = runTest(timeout = 120.seconds) {
        // dedicated PG so we can turn on commit-timestamp tracking and read the exact commit time
        val pg = newDedicatedPostgres()
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "track_commit_timestamp=on")
        pg.start()
        try {
            val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
            val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
            val sourceTopic = "test-topic-${UUID.randomUUID()}"

            pg.executeSql(
                "CREATE TABLE pg_systime (_id INT PRIMARY KEY, name TEXT)",
                "CREATE PUBLICATION $pubName FOR TABLE pg_systime",
            )

            openNode(sourceTopic, pgContainer = pg).use { node ->
                attachPostgresSource(node, slotName = slotName, publicationName = pubName)

                // wait until the snapshot is done and streaming is live, so the insert below streams
                eventually(60.seconds) {
                    assertTrue(
                        (node as XtdbInternal).dbCatalog["cdc"]?.watchers?.externalSourceToken
                            ?.let { PostgresSourceToken.parseFrom(it).snapshotCompleted } == true,
                        "streaming live"
                    )
                }

                pg.executeSql("INSERT INTO pg_systime (_id, name) VALUES (1, 'streamed')")
                eventually(60.seconds) {
                    assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_systime WHERE _id = 1").isNotEmpty(), "streamed row visible")
                }

                // pgwire hands these back as different types from the two connections —
                // PG's commit ts as java.sql.Timestamp, XTDB's _system_from as ZonedDateTime —
                // so compare them as the absolute instants they represent.
                val pgCommit = DriverManager.getConnection(pg.jdbcUrl, pg.username, pg.password).use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.executeQuery(
                            "SELECT pg_xact_commit_timestamp(xmin) AS ts FROM pg_systime WHERE _id = 1").use { rs ->
                            check(rs.next()); (rs.getObject("ts") as java.sql.Timestamp).toInstant()
                        }
                    }
                }
                val sysFrom = (xtQueryDb(node, "cdc",
                    "SELECT _system_from FROM public.pg_systime WHERE _id = 1")[0]["_system_from"]
                    as java.time.ZonedDateTime).toInstant()

                assertEquals(pgCommit, sysFrom,
                    "streamed tx _system_from should equal the Postgres commit timestamp")
            }
        } finally {
            pg.stop()
        }
    }

    @Test
    fun `snapshot tx system-time is xtdb-assigned, not the postgres commit time`() = runTest(timeout = 120.seconds) {
        val pubName = "test_pub_${UUID.randomUUID().toString().replace("-", "_")}"
        val slotName = "test_slot_${UUID.randomUUID().toString().replace("-", "_")}"
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        pgExecute(
            "CREATE TABLE pg_snap_systime (_id INT PRIMARY KEY, name TEXT)",
            "CREATE PUBLICATION $pubName FOR TABLE pg_snap_systime",
            "INSERT INTO pg_snap_systime (_id, name) VALUES (1, 'preexisting')",
        )

        val committedAround = java.time.Instant.now()
        // a real wall-clock gap (delay() would be skipped under runTest) so the snapshot's
        // index-time can't be confused with the row's earlier commit time
        runInterruptible { Thread.sleep(2_000) }

        openNode(sourceTopic).use { node ->
            attachPostgresSource(node, slotName = slotName, publicationName = pubName)

            eventually(60.seconds) {
                assertTrue(xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_snap_systime WHERE _id = 1").isNotEmpty(), "snapshot row visible")
            }

            // XTDB returns _system_from (timestamptz) as a ZonedDateTime over pgwire
            val sysFrom = (xtQueryDb(node, "cdc",
                "SELECT _system_from FROM public.pg_snap_systime WHERE _id = 1")[0]["_system_from"]
                as java.time.ZonedDateTime).toInstant()

            assertTrue(java.time.Duration.between(committedAround, sysFrom).seconds >= 1,
                "snapshot _system_from ($sysFrom) should be the XTDB-assigned time, well after the PG commit (~$committedAround)")
        }
    }
}
