package xtdb.debezium

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.log.Log.Companion.tailAll
import xtdb.api.log.SourceMessage
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.DriverManager
import java.time.Duration
import java.util.Collections
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class DebeziumIntegrationTest {

    private lateinit var network: Network
    private lateinit var kafka: ConfluentKafkaContainer
    private lateinit var debeziumConnect: GenericContainer<*>
    private lateinit var postgres: PostgreSQLContainer

    private val httpClient: HttpClient = HttpClient.newHttpClient()

    @BeforeEach
    fun setUp() {
        network = Network.newNetwork()

        postgres = PostgreSQLContainer("postgres:17-alpine")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical")

        kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092")

        debeziumConnect = GenericContainer("quay.io/debezium/connect:3.0")
            .withNetwork(network)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("GROUP_ID", "debezium-connect")
            .withEnv("CONFIG_STORAGE_TOPIC", "debezium_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "debezium_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "debezium_statuses")
            .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200))
            .dependsOn(kafka)

        Startables.deepStart(postgres, kafka, debeziumConnect).join()
    }

    @AfterEach
    fun tearDown() {
        debeziumConnect.stop()
        kafka.stop()
        postgres.stop()
        network.close()
    }

    private fun kafkaConfig() = mapOf("bootstrap.servers" to kafka.bootstrapServers)

    private fun connectUrl() =
        "http://${debeziumConnect.host}:${debeziumConnect.getMappedPort(8083)}"

    private suspend fun registerConnectorAndAwait(schemas: String = "public") {
        fun isConnectorRunning(): Boolean {
            try {
                val resp = httpClient.send(
                    HttpRequest.newBuilder()
                        .uri(URI.create("${connectUrl()}/connectors/test-connector/status"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                )
                if (resp.statusCode() != 200) return false
                val status = Json.parseToJsonElement(resp.body()).jsonObject
                val connectorState = status["connector"]?.jsonObject?.get("state")?.jsonPrimitive?.content
                val taskState = status["tasks"]?.jsonArray?.firstOrNull()?.jsonObject?.get("state")?.jsonPrimitive?.content
                return connectorState == "RUNNING" && taskState == "RUNNING"
            } catch (_: Exception) {
                return false
            }
        }

        val connectorConfig = buildJsonObject {
            put("name", "test-connector")
            putJsonObject("config") {
                put("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                put("tasks.max", "1")
                put("database.hostname", "postgres")
                put("database.port", "5432")
                put("database.user", "testuser")
                put("database.password", "testpass")
                put("database.dbname", "testdb")
                put("topic.prefix", "testdb")
                put("schema.include.list", schemas)
                put("plugin.name", "pgoutput")
            }
        }

        val request = HttpRequest.newBuilder()
            .uri(URI.create("${connectUrl()}/connectors"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(connectorConfig.toString()))
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        assertTrue(response.statusCode() in 200..201, "Failed to register connector: ${response.body()}")

        while (!isConnectorRunning()) delay(500)
    }

    private fun pgExecute(vararg statements: String) {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                for (sql in statements) stmt.execute(sql)
            }
        }
    }

    private fun xtQuery(node: Xtdb, sql: String): List<Map<String, Any?>> =
        xtQueryDb(node, "xtdb", sql)

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

    private suspend fun pollMessages(topic: String, expected: Int): List<JsonObject> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "test-consumer",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        )

        return KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(topic))

            val messages = mutableListOf<JsonObject>()

            while (messages.size < expected) {
                val records = consumer.poll(Duration.ofSeconds(1))
                for (record in records) {
                    val value = record.value() ?: continue
                    messages.add(Json.parseToJsonElement(value).jsonObject)
                }
                delay(100)
            }

            assertEquals(expected, messages.size, "Expected $expected CDC messages on $topic, got ${messages.size}")
            messages
        }
    }

    private fun JsonObject.payload(): JsonObject =
        this["payload"]?.jsonObject ?: fail("Expected 'payload' key in message")

    @Test
    fun `debezium captures full CDC lifecycle`() = runTest(timeout = 120.seconds) {
        fun assertCdcEvent(message: JsonObject, expectedOp: String, after: JsonObject? = null) {
            val payload = message.payload()
            assertEquals(expectedOp, payload["op"]?.jsonPrimitive?.content)
            assertEquals(after, payload["after"]?.takeUnless { it is JsonNull }?.jsonObject)
        }

        // Initial (s)napshot
        pgExecute(
            "CREATE TABLE IF NOT EXISTS test_items (id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO test_items (id, name) VALUES (1, 'snapshot-row')",
        )

        registerConnectorAndAwait()

        // (c)reate, (u)pdate, (d)elete events
        pgExecute(
            "INSERT INTO test_items (id, name) VALUES (2, 'inserted')",
            "UPDATE test_items SET name = 'updated' WHERE id = 2",
            "DELETE FROM test_items WHERE id = 2",
        )

        val messages = pollMessages("testdb.public.test_items", expected = 4)

        assertCdcEvent(messages[0], "r", after = buildJsonObject { put("id", 1); put("name", "snapshot-row") })
        assertCdcEvent(messages[1], "c", after = buildJsonObject { put("id", 2); put("name", "inserted") })
        assertCdcEvent(messages[2], "u", after = buildJsonObject { put("id", 2); put("name", "updated") })
        assertCdcEvent(messages[3], "d")
    }

    @Test
    fun `debezium type mapping for PG temporal and structural types`() = runTest(timeout = 120.seconds) {
        pgExecute(
            """CREATE TABLE IF NOT EXISTS type_probe (
                _id INT PRIMARY KEY,
                ts TIMESTAMP,
                tstz TIMESTAMPTZ,
                d DATE,
                t TIME,
                b BOOLEAN,
                i INT,
                bi BIGINT,
                f FLOAT,
                dp DOUBLE PRECISION,
                txt TEXT,
                j JSON,
                jb JSONB
            )""",
        )

        registerConnectorAndAwait()

        pgExecute(
            """INSERT INTO type_probe (_id, ts, tstz, d, t, b, i, bi, f, dp, txt, j, jb)
               VALUES (
                   1,
                   '2024-01-01 00:00:00',
                   '2024-01-01 00:00:00+00',
                   '2024-01-01',
                   '12:30:00',
                   true,
                   42,
                   9999999999,
                   3.14,
                   2.718281828,
                   'hello',
                   '{"key": "val"}',
                   '{"key": "val"}'
               )""",
        )

        val messages = pollMessages("testdb.public.type_probe", expected = 1)
        val after = messages[0].payload()["after"]!!.jsonObject

        // Temporal types — TIMESTAMP is epoch micros, TIMESTAMPTZ is ISO string
        assertEquals(1704067200000000L, after["ts"]!!.jsonPrimitive.longOrNull, "TIMESTAMP should be epoch micros")
        assertTrue(after["tstz"]!!.jsonPrimitive.isString, "TIMESTAMPTZ should be a string")
        assertTrue(after["tstz"]!!.jsonPrimitive.content.endsWith("Z"), "TIMESTAMPTZ should be ISO-8601 UTC")
        assertEquals(19723L, after["d"]!!.jsonPrimitive.longOrNull, "DATE should be days since epoch")
        assertEquals(45000000000L, after["t"]!!.jsonPrimitive.longOrNull, "TIME should be nanos since midnight")

        // Numeric types
        assertEquals(42L, after["i"]!!.jsonPrimitive.longOrNull, "INT should be a long")
        assertEquals(9999999999L, after["bi"]!!.jsonPrimitive.longOrNull, "BIGINT should be a long")
        assertEquals(3.14, after["f"]!!.jsonPrimitive.doubleOrNull, "FLOAT should be a double")
        assertEquals(2.718281828, after["dp"]!!.jsonPrimitive.doubleOrNull, "DOUBLE should be a double")

        // Text/JSON types — both JSON and JSONB come through as strings
        assertEquals("hello", after["txt"]!!.jsonPrimitive.content)
        assertTrue(after["j"]!!.jsonPrimitive.isString, "JSON should be a string")
        assertTrue(after["jb"]!!.jsonPrimitive.isString, "JSONB should be a string")

        // Boolean
        assertEquals(true, after["b"]!!.jsonPrimitive.booleanOrNull, "BOOLEAN should be a boolean")
    }

    @Test
    fun `CDC events are ingested into XTDB`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS cdc_users (_id INT PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO cdc_users (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )

        registerConnectorAndAwait()

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val log = DebeziumLog(kafkaConfig(), "testdb.public.cdc_users")
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    pgExecute(
                        "INSERT INTO cdc_users (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                        "UPDATE cdc_users SET email = 'alice-new@example.com' WHERE _id = 1",
                        "DELETE FROM cdc_users WHERE _id = 2",
                    )

                    // snapshot(Alice) + insert(Bob) + update(Alice) + delete(Bob)
                    while (received.size < 4) delay(100)

                    // Allow indexing to complete
                    delay(2000)
                }
            }

            val history = xtQuery(node,
                """SELECT _id, name, email, _valid_from, _valid_to
                   FROM public.cdc_users
                   FOR ALL VALID_TIME
                   ORDER BY _id, _valid_from"""
            )

            assertEquals(3, history.size, "Expected 3 history rows (2 Alice + 1 Bob)")

            // Alice: snapshot row then updated row
            assertEquals(1L, (history[0]["_id"] as Number).toLong())
            assertEquals("alice@example.com", history[0]["email"])
            assertTrue(history[0]["_valid_to"] != null, "Snapshot row should be superseded")

            assertEquals(1L, (history[1]["_id"] as Number).toLong())
            assertEquals("alice-new@example.com", history[1]["email"])
            assertNull(history[1]["_valid_to"], "Updated row should be current")

            // Bob: inserted then deleted — valid_to proves DELETE op worked
            assertEquals(2L, (history[2]["_id"] as Number).toLong())
            assertEquals("bob@example.com", history[2]["email"])
            assertTrue(history[2]["_valid_to"] != null, "Bob should have valid_to from DELETE")
        }
    }

    @Test
    fun `valid_from and valid_to columns are used as valid time bounds`() = runTest(timeout = 120.seconds) {
        pgExecute(
            """CREATE TABLE IF NOT EXISTS timed_docs (
                _id INT PRIMARY KEY,
                name TEXT,
                _valid_from TIMESTAMPTZ,
                _valid_to TIMESTAMPTZ
            )""",
        )

        registerConnectorAndAwait()

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val log = DebeziumLog(kafkaConfig(), "testdb.public.timed_docs")
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    pgExecute(
                        """INSERT INTO timed_docs (_id, name, _valid_from, _valid_to)
                           VALUES (1, 'bounded', '2024-01-01T00:00:00Z', '2025-01-01T00:00:00Z')""",
                        """INSERT INTO timed_docs (_id, name, _valid_from)
                           VALUES (2, 'from-only', '2024-06-01T00:00:00Z')""",
                        """INSERT INTO timed_docs (_id, name, _valid_to)
                           VALUES (3, 'to-only', '2025-06-01T00:00:00Z')""",
                        """INSERT INTO timed_docs (_id, name)
                           VALUES (4, 'neither')""",
                    )

                    while (received.size < 4) delay(100)
                    delay(2000)
                }
            }

            val rows = xtQuery(node,
                """SELECT _id, name, _valid_from, _valid_to
                   FROM public.timed_docs
                   FOR ALL VALID_TIME
                   ORDER BY _id"""
            )

            // 3 rows ingested — _id=3 (to-only) rejected to DLQ
            assertEquals(3, rows.size)

            // Both bounds set
            assertEquals(1L, (rows[0]["_id"] as Number).toLong())
            assertEquals("bounded", rows[0]["name"])
            assertTrue(rows[0]["_valid_from"] != null, "Should have valid_from")
            assertTrue(rows[0]["_valid_to"] != null, "Should have valid_to")

            // Only valid_from
            assertEquals(2L, (rows[1]["_id"] as Number).toLong())
            assertEquals("from-only", rows[1]["name"])
            assertTrue(rows[1]["_valid_from"] != null, "Should have valid_from")
            assertNull(rows[1]["_valid_to"], "Should have no valid_to")

            // Neither — system-assigned valid_from, no valid_to
            assertEquals(4L, (rows[2]["_id"] as Number).toLong())
            assertEquals("neither", rows[2]["name"])
            assertTrue(rows[2]["_valid_from"] != null, "Should have system-assigned valid_from")
            assertNull(rows[2]["_valid_to"], "Should have no valid_to")

            // _valid_to without _valid_from → DLQ
            val dlqTxs = xtQuery(node,
                """SELECT (user_metadata).error
                   FROM xt.txs
                   WHERE (user_metadata).source = 'debezium'
                     AND (user_metadata).error IS NOT NULL"""
            )
            assertEquals(1, dlqTxs.size, "Expected 1 DLQ transaction for to-only record")
            assertTrue(
                (dlqTxs[0]["error"] as String).contains("_valid_from"),
                "DLQ error should mention _valid_from"
            )
        }
    }

    @Test
    fun `table without _id column sends records to DLQ`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS no_id_table (id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO no_id_table (id, name) VALUES (1, 'pre-existing')",
        )

        registerConnectorAndAwait()

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val log = DebeziumLog(kafkaConfig(), "testdb.public.no_id_table")
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    pgExecute(
                        "INSERT INTO no_id_table (id, name) VALUES (2, 'also-no-_id')",
                    )

                    // snapshot + insert = 2 records, both should fail and go to DLQ
                    while (received.size < 2) delay(100)
                    delay(2000)
                }
            }

            // No rows should be ingested — all went to DLQ
            val rows = xtQuery(node,
                "SELECT * FROM public.no_id_table FOR ALL VALID_TIME"
            )
            assertEquals(0, rows.size, "No rows should be ingested — all records lack _id")

            // DLQ txs have source=debezium and error in user_metadata
            val dlqTxs = xtQuery(node,
                """SELECT _id, (user_metadata).source, (user_metadata).error
                   FROM xt.txs
                   WHERE (user_metadata).source = 'debezium'
                     AND (user_metadata).error IS NOT NULL
                   ORDER BY _id"""
            )
            assertEquals(2, dlqTxs.size, "Expected 2 DLQ transactions, got ${dlqTxs.size}")
            assertTrue(
                (dlqTxs[0]["error"] as String).contains("_id"),
                "DLQ error should mention missing _id"
            )
        }
    }

    @Test
    fun `various PG types are ingested correctly`() = runTest(timeout = 120.seconds) {
        pgExecute(
            """CREATE TABLE IF NOT EXISTS typed_docs (
                _id INT PRIMARY KEY,
                name TEXT,
                score DOUBLE PRECISION,
                active BOOLEAN,
                metadata JSONB,
                tags TEXT[],
                notes TEXT
            )""",
        )

        registerConnectorAndAwait()

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val log = DebeziumLog(kafkaConfig(), "testdb.public.typed_docs")
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    pgExecute(
                        """INSERT INTO typed_docs (_id, name, score, active, metadata, tags, notes)
                           VALUES (1, 'Alice', 3.14, true, '{"city": "London", "nested": {"deep": true}}',
                                   ARRAY['admin', 'user'], NULL)""",
                        """INSERT INTO typed_docs (_id, name, score, active, metadata, tags, notes)
                           VALUES (2, 'Bob', NULL, false, NULL, NULL, 'some notes')""",
                    )

                    while (received.size < 2) delay(100)
                    delay(2000)
                }
            }

            val rows = xtQuery(node,
                """SELECT _id, name, score, active, metadata, tags, notes
                   FROM public.typed_docs
                   ORDER BY _id"""
            )

            assertEquals(2, rows.size)

            // Alice: all fields populated
            val alice = rows[0]
            assertEquals(1L, (alice["_id"] as Number).toLong())
            assertEquals("Alice", alice["name"])
            assertEquals(3.14, (alice["score"] as Number).toDouble(), 0.001)
            assertEquals(true, alice["active"])
            // JSONB comes through Debezium as a string
            assertTrue(alice["metadata"] is String, "JSONB should arrive as a string, got: ${alice["metadata"]?.javaClass}")
            assertNull(alice["notes"], "NULL column should be null")

            // Bob: sparse row with NULLs
            val bob = rows[1]
            assertEquals(2L, (bob["_id"] as Number).toLong())
            assertEquals("Bob", bob["name"])
            assertNull(bob["score"], "NULL double should be null")
            assertEquals(false, bob["active"])
            assertNull(bob["metadata"], "NULL JSONB should be null")
            assertEquals("some notes", bob["notes"])
        }
    }

    @Test
    fun `non-public schema is preserved in XTDB`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE SCHEMA IF NOT EXISTS inventory",
            "CREATE TABLE IF NOT EXISTS inventory.products (_id INT PRIMARY KEY, name TEXT, qty INT)",
        )

        registerConnectorAndAwait(schemas = "inventory")

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val log = DebeziumLog(kafkaConfig(), "testdb.inventory.products")
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    pgExecute(
                        "INSERT INTO inventory.products (_id, name, qty) VALUES (1, 'Widget', 100)",
                    )

                    while (received.size < 1) delay(100)
                    delay(2000)
                }
            }

            val rows = xtQuery(node,
                "SELECT _id, name, qty FROM inventory.products"
            )

            assertEquals(1, rows.size)
            assertEquals(1L, (rows[0]["_id"] as Number).toLong())
            assertEquals("Widget", rows[0]["name"])
            assertEquals(100L, (rows[0]["qty"] as Number).toLong())
        }
    }

    @Test
    fun `CDC events are ingested into secondary database`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS cdc_secondary_test (_id INT PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO cdc_secondary_test (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )

        registerConnectorAndAwait()

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            // Attach a secondary database
            node.getConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ATTACH DATABASE cdc_secondary")
                }
            }

            val log = DebeziumLog(kafkaConfig(), "testdb.public.cdc_secondary_test")
            val processor = DebeziumProcessor(node, "cdc_secondary", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    pgExecute(
                        "INSERT INTO cdc_secondary_test (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                    )

                    // snapshot(Alice) + insert(Bob) = 2 records
                    while (received.size < 2) delay(100)
                    delay(2000)
                }
            }

            // Secondary db should have the rows
            val rows = xtQueryDb(node, "cdc_secondary",
                """SELECT _id, name, email
                   FROM public.cdc_secondary_test
                   ORDER BY _id"""
            )
            assertEquals(2, rows.size, "Expected 2 rows in secondary database")
            assertEquals(1L, (rows[0]["_id"] as Number).toLong())
            assertEquals("Alice", rows[0]["name"])
            assertEquals(2L, (rows[1]["_id"] as Number).toLong())
            assertEquals("Bob", rows[1]["name"])

            // Primary db should have no debezium txs
            val primaryTxs = xtQuery(node,
                """SELECT _id FROM xt.txs
                   WHERE (user_metadata).source = 'debezium'"""
            )
            assertEquals(0, primaryTxs.size, "Primary database should have no debezium transactions")
        }
    }

    @Test
    fun `non-TIMESTAMPTZ _valid_from goes to DLQ`() = runTest(timeout = 120.seconds) {
        pgExecute(
            """CREATE TABLE IF NOT EXISTS bad_times (
                _id INT PRIMARY KEY,
                name TEXT,
                _valid_from TIMESTAMP,
                _valid_to TEXT
            )""",
        )

        registerConnectorAndAwait()

        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val log = DebeziumLog(kafkaConfig(), "testdb.public.bad_times")
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)
            val received = Collections.synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

            val capturing = object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Log.Record<SourceMessage>>) {
                    processor.processRecords(records)
                    received.addAll(records)
                }
            }

            log.use {
                log.tailAll(-1, capturing).use {
                    // TIMESTAMP (no tz) → Debezium sends as Long, not a string
                    pgExecute(
                        """INSERT INTO bad_times (_id, name, _valid_from)
                           VALUES (1, 'wrong-type', '2024-01-01 00:00:00')""",
                    )
                    // TEXT with non-ISO content → string that won't parse
                    pgExecute(
                        """INSERT INTO bad_times (_id, name, _valid_to)
                           VALUES (2, 'bad-string', 'not-a-date')""",
                    )

                    while (received.size < 2) delay(100)
                    delay(2000)
                }
            }

            // Neither row should be ingested
            val rows = xtQuery(node,
                "SELECT * FROM public.bad_times FOR ALL VALID_TIME"
            )
            assertEquals(0, rows.size, "No rows should be ingested — both have invalid valid time")

            val dlqTxs = xtQuery(node,
                """SELECT (user_metadata).error
                   FROM xt.txs
                   WHERE (user_metadata).source = 'debezium'
                     AND (user_metadata).error IS NOT NULL
                   ORDER BY _id"""
            )
            assertEquals(2, dlqTxs.size, "Expected 2 DLQ transactions")
            assertTrue(
                (dlqTxs[0]["error"] as String).contains("_valid_from"),
                "First DLQ should mention _valid_from"
            )
            assertTrue(
                (dlqTxs[1]["error"] as String).contains("_valid_to"),
                "Second DLQ should mention _valid_to"
            )
        }
    }
}
