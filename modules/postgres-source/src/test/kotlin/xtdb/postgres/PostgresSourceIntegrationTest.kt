package xtdb.postgres

import kotlinx.coroutines.delay
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import java.sql.DriverManager
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
@EnabledIfEnvironmentVariable(named = "XTDB_SINGLE_WRITER", matches = "true")
class PostgresSourceIntegrationTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val postgres = PostgreSQLContainer("postgres:17-alpine")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical")

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
                          schemaIncludeList: [public]
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
}
