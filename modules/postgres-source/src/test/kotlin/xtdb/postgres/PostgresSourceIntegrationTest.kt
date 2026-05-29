package xtdb.postgres

import clojure.lang.ILookup
import clojure.lang.Keyword
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.api.Xtdb
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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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

    private fun pgExecute(vararg statements: String) {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                for (sql in statements) stmt.execute(sql)
            }
        }
    }

    private fun openNode(sourceTopic: String): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
        remote("pg", PostgresRemote.Factory(
            hostname = postgres.host,
            port = postgres.getMappedPort(5432),
            database = "testdb",
            username = "testuser",
            password = "testpass",
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
                          topic: test-replica-${UUID.randomUUID()}
                        externalSource: !Postgres
                          remote: pg
                          slotName: $slotName
                          publicationName: $publicationName
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

    private suspend fun awaitCondition(description: String, timeout: Duration = 10.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds

        while (System.currentTimeMillis() < deadline) {
            if (check()) return
            runInterruptible { Thread.sleep(200) }
        }

        fail("Timed out waiting for: $description")
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

            awaitCondition("Bob appears", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_resume WHERE _id = 2").isNotEmpty()
            }
        }

        // Insert while node is down — this must appear after restart
        pgExecute("INSERT INTO pg_resume (_id, name) VALUES (3, 'Charlie')")

        // Phase 2: restart with the same Kafka source topic.
        // The node replays the ATTACH DATABASE from the source log,
        // recovers the CDC token (snapshotCompleted=true), and resumes streaming.
        openNode(sourceTopic).use { node ->
            awaitCondition("Charlie appears after restart", timeout = 60.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_resume WHERE _id = 3").isNotEmpty()
                }.getOrDefault(false)
            }

            val rows = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.pg_resume ORDER BY _id")
            assertEquals(3, rows.size, "All three rows should be present — no duplication, no loss")
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])
            assertEquals("Charlie", rows[2]["name"])
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
                awaitCondition("all streamed rows ingested", timeout = 60.seconds) {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_diverge").size == 21
                }

                // Flush a block so the cdc block catalog persists latestCompletedTx (high txId)
                // and latestProcessedMsgId (low source-log offset) to durable storage.
                val cdc = (node as Xtdb.XtdbInternal).dbCatalog["cdc"]!!
                cdc.sendFlushBlockMessage()
                awaitCondition("block persisted for cdc", timeout = 30.seconds) {
                    cdc.blockCatalog.currentBlockIndex != null
                }

                val txId = cdc.blockCatalog.latestCompletedTx!!.txId
                val sourceOffset = cdc.sourceLog.latestSubmittedOffset
                assertTrue(
                    txId > sourceOffset,
                    "test precondition: txId ($txId) should exceed source-log offset ($sourceOffset)"
                )
            }

            // Phase 2: restart against the same source topic + persisted storage.
            // Pre-31b825623 openNode would throw IllegalStateException (due to us validating offsets against txId)
            // and the node would fail to start.
            openNode(sourceTopic).use { node ->
                awaitCondition("cdc db queryable after restart", timeout = 60.seconds) {
                    runCatching {
                        xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_diverge").size == 21
                    }.getOrDefault(false)
                }
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
            awaitCondition("streamed row visible") {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_metrics WHERE _id = 2").isNotEmpty()
            }

            fun metric(name: String) =
                metrics.find(name).tags("db", "cdc", "source", slotName, "source_type", "postgres")

            assertTrue((metric("xtdb.postgres_source.events.total").counter()?.count() ?: 0.0) >= 1.0,
                "events.total should have counted the streamed row")
            assertTrue((metric("xtdb.postgres_source.commits.total").counter()?.count() ?: 0.0) >= 1.0,
                "commits.total should have counted the streamed commit")

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
            awaitCondition("both tables snapshotted", timeout = 30.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_users").size == 2 &&
                        xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_orders").size == 1
                }.getOrDefault(false)
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

            awaitCondition("streaming changes to both tables", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_users WHERE _id = 3").isNotEmpty() &&
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_mt_orders WHERE _id = 2").isNotEmpty()
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

            awaitCondition("included table snapshotted", timeout = 30.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_pub_in WHERE _id = 1").isNotEmpty()
                }.getOrDefault(false)
            }

            // Stream a change to both tables; only the included one should land.
            pgExecute(
                "INSERT INTO pg_pub_in (_id, name) VALUES (2, 'in-stream')",
                "INSERT INTO pg_pub_out (_id, name) VALUES (2, 'out-stream')",
            )

            awaitCondition("streaming change to included table", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_pub_in WHERE _id = 2").isNotEmpty()
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

            awaitCondition("Alice snapshotted", timeout = 30.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_evolve WHERE _id = 1").isNotEmpty()
                }.getOrDefault(false)
            }

            // ALTER TABLE in PG while streaming is active
            pgExecute(
                "ALTER TABLE pg_evolve ADD COLUMN email TEXT",
                "INSERT INTO pg_evolve (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
            )

            awaitCondition("Bob with new column appears", timeout = 30.seconds) {
                val rows = xtQueryDb(node, "cdc", "SELECT _id, email FROM public.pg_evolve WHERE _id = 2")
                rows.isNotEmpty() && rows[0]["email"] == "bob@example.com"
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

            awaitCondition("Alice updated", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT email FROM public.pg_cdc_users WHERE _id = 1")
                    .firstOrNull()?.get("email") == "alice-new@example.com"
            }

            val bob = xtQueryDb(node, "cdc", "SELECT email FROM public.pg_cdc_users WHERE _id = 2")
            assertEquals(1, bob.size)
            assertEquals("bob@example.com", bob[0]["email"])

            // Phase 3: delete
            pgExecute("DELETE FROM pg_cdc_users WHERE _id = 2")

            awaitCondition("Bob deleted", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_cdc_users WHERE _id = 2").isEmpty()
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

            awaitCondition("snapshot row ingested", timeout = 30.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_types WHERE _id = 1").isNotEmpty()
                }.getOrDefault(false)
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

            awaitCondition("streaming row ingested", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_types WHERE _id = 2").isNotEmpty()
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
            awaitCondition("Bob appears", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_cancel WHERE _id = 2").isNotEmpty()
            }

            // Streaming connection is now holding the slot
            assertEquals(true, pgSlotState(slotName), "Slot should be active while streaming")
        }
        // node.close() triggers LeaderLogProcessor.close() → extJob.cancel()
        // The closeOnCancel child coroutine should force-close the PG connection,
        // releasing the replication slot.

        awaitCondition("slot released after node close", timeout = 10.seconds) {
            pgSlotState(slotName) == false
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
            awaitCondition("v0 ingested", timeout = 30.seconds) {
                runCatching { xtQueryDb(node, "cdc", "SELECT _id FROM public.pg_collapse WHERE _id = 1").isNotEmpty() }
                    .getOrDefault(false)
            }

            pgExecute("BEGIN", "UPDATE pg_collapse SET name = 'v1' WHERE _id = 1", "COMMIT")
            pgExecute("BEGIN", "UPDATE pg_collapse SET name = 'v2' WHERE _id = 1", "COMMIT")
            pgExecute("BEGIN", "UPDATE pg_collapse SET name = 'v3' WHERE _id = 1", "COMMIT")

            awaitCondition("final update applied", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT name FROM public.pg_collapse WHERE _id = 1")
                    .firstOrNull()?.get("name") == "v3"
            }

            val history = xtQueryDb(node, "cdc", "SELECT name FROM public.pg_collapse FOR ALL VALID_TIME WHERE _id = 1 ORDER BY _valid_from")
                .map { it["name"] as String }

            assertEquals(listOf("v0", "v1", "v2", "v3"), history)
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
}
