package xtdb.debezium

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.io.TempDir
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
import xtdb.api.log.IngestionStoppedException
import xtdb.api.log.KafkaCluster
import java.util.UUID
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.DriverManager
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
@EnabledIfEnvironmentVariable(named = "XTDB_SINGLE_WRITER", matches = "true")
class DebeziumIntegrationTest {

    private val httpClient: HttpClient = HttpClient.newHttpClient()
    private val connectorName = "test-connector-${UUID.randomUUID()}"

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
            .withListener("kafka:19092")

        private val debeziumConnect = GenericContainer("quay.io/debezium/connect:3.0")
            .withNetwork(network)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("GROUP_ID", "debezium-connect")
            .withEnv("CONFIG_STORAGE_TOPIC", "debezium_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "debezium_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "debezium_statuses")
            .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200))
            .dependsOn(kafka)

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            Startables.deepStart(postgres, kafka, debeziumConnect).join()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            debeziumConnect.stop()
            kafka.stop()
            postgres.stop()
            network.close()
        }
    }

    @AfterEach
    fun cleanUpConnector() {
        try {
            httpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("${connectUrl()}/connectors/$connectorName"))
                    .DELETE()
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            )

            val deadline = System.currentTimeMillis() + 10_000
            while (System.currentTimeMillis() < deadline) {
                val resp = httpClient.send(
                    HttpRequest.newBuilder()
                        .uri(URI.create("${connectUrl()}/connectors/$connectorName/status"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                )
                if (resp.statusCode() == 404) break
                Thread.sleep(200)
            }
        } catch (_: Exception) {}
    }

    private fun kafkaConfig() = mapOf("bootstrap.servers" to kafka.bootstrapServers)

    private fun connectUrl() =
        "http://${debeziumConnect.host}:${debeziumConnect.getMappedPort(8083)}"

    private suspend fun registerConnectorAndAwait(
        schemas: String = "public",
        extraConfig: Map<String, String> = emptyMap(),
        awaitTopics: List<String> = emptyList(),
    ) {
        fun isConnectorRunning(): Boolean {
            try {
                val resp = httpClient.send(
                    HttpRequest.newBuilder()
                        .uri(URI.create("${connectUrl()}/connectors/$connectorName/status"))
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
            put("name", connectorName)
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
                put("slot.name", "debezium_${connectorName.replace("-", "_")}")
                put("slot.drop.on.stop", "true")
                for ((k, v) in extraConfig) put(k, v)
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

        if (awaitTopics.isNotEmpty()) {
            org.apache.kafka.clients.admin.AdminClient.create(kafkaConfig()).use { admin ->
                val deadline = System.currentTimeMillis() + 30_000
                while (System.currentTimeMillis() < deadline) {
                    val names = admin.listTopics().names().get()
                    if (awaitTopics.all { it in names }) return
                    delay(500)
                }
                val missing = awaitTopics.filterNot { it in admin.listTopics().names().get() }
                throw AssertionError("Timed out waiting for topics: $missing")
            }
        }
    }

    private fun pgExecute(vararg statements: String) {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                for (sql in statements) stmt.execute(sql)
            }
        }
    }

    private fun openNodeOnSourceTopic(
        sourceTopic: String,
        kafkaProperties: Map<String, String> = emptyMap(),
    ): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers).propertiesMap(kafkaProperties))
        log(KafkaCluster.LogFactory("kafka", sourceTopic))
    }

    private fun createTopic(topic: String) {
        org.apache.kafka.clients.admin.AdminClient.create(kafkaConfig()).use { admin ->
            admin.createTopics(listOf(
                org.apache.kafka.clients.admin.NewTopic(topic, 1, 1.toShort())
            )).all().get()
        }
    }

    private fun attachDebeziumDb(
        node: Xtdb,
        debeziumTopic: String,
        dbName: String = "cdc",
        storagePath: java.nio.file.Path? = null,
    ) {
        val storageYaml = if (storagePath != null) """
                    storage: !Local
                      path: $storagePath""" else ""
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""ATTACH DATABASE $dbName WITH $$
                    log: !Kafka
                      cluster: kafka
                      topic: test-replica-${UUID.randomUUID()}$storageYaml
                    externalSource: !Debezium
                      messageFormat: !Json {}
                      log: !Kafka
                        logCluster: kafka
                        tableTopic: $debeziumTopic
                $$""")
            }
        }
    }

    private suspend fun awaitTxs(node: Xtdb, expected: Int, db: String = "xtdb", timeout: Long = 10_000) {
        val deadline = System.currentTimeMillis() + timeout
        var count = 0L
        while (System.currentTimeMillis() < deadline) {
            count = xtQueryDb(node, db, "SELECT count(*) AS cnt FROM xt.txs")[0]["cnt"] as Long
            if (count >= expected) return
            delay(200)
        }
        throw AssertionError("Timed out waiting for $expected txs on db '$db' (got $count)")
    }

    private suspend fun awaitCondition(description: String, timeout: Duration = 10.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (System.currentTimeMillis() < deadline) {
            if (check()) return
            delay(200)
        }
        throw AssertionError("Timed out waiting for: $description")
    }

    private suspend fun awaitDatabase(node: Xtdb, dbName: String, timeout: Long = 30_000) {
        val deadline = System.currentTimeMillis() + timeout
        while (System.currentTimeMillis() < deadline) {
            if ((node as Xtdb.XtdbInternal).dbCatalog.databaseOrNull(dbName) != null) return
            delay(200)
        }
        throw AssertionError("Timed out waiting for database '$dbName'")
    }

    private fun database(node: Xtdb, dbName: String) =
        (node as Xtdb.XtdbInternal).dbCatalog.databaseOrNull(dbName)
            ?: throw AssertionError("Database '$dbName' does not exist")

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
                val records = consumer.poll(java.time.Duration.ofSeconds(1))
                for (record in records) {
                    val value = record.value() ?: continue
                    messages.add(Json.parseToJsonElement(value).jsonObject)
                }
                runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
            }

            assertEquals(expected, messages.size, "Expected $expected CDC messages on $topic, got ${messages.size}")
            messages
        }
    }

    private fun JsonObject.payload(): JsonObject =
        this["payload"]?.jsonObject ?: fail("Expected 'payload' key in message")


    @Test
    fun `debezium captures full CDC lifecycle`() = runTest(timeout = 120.seconds) {
        fun assertCdcData(message: JsonObject, after: JsonObject? = null) {
            val payload = message.payload()
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

        assertCdcData(messages[0], after = buildJsonObject { put("id", 1); put("name", "snapshot-row") })
        assertCdcData(messages[1], after = buildJsonObject { put("id", 2); put("name", "inserted") })
        assertCdcData(messages[2], after = buildJsonObject { put("id", 2); put("name", "updated") })
        assertCdcData(messages[3])
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

        registerConnectorAndAwait(awaitTopics = listOf("testdb.public.cdc_users"))

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachDebeziumDb(node, "testdb.public.cdc_users")

            // Phase 1: snapshot ingested
            awaitTxs(node, 1, db = "cdc")

            val snapshotRows = xtQueryDb(node, "cdc",
                "SELECT _id, name, email FROM public.cdc_users ORDER BY _id"
            )
            assertEquals(1, snapshotRows.size, "Snapshot should ingest Alice")
            assertEquals("alice@example.com", snapshotRows[0]["email"])

            // Phase 2: streaming events — happens-before guarantees distinct system times
            pgExecute(
                "INSERT INTO cdc_users (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                "UPDATE cdc_users SET email = 'alice-new@example.com' WHERE _id = 1",
                "DELETE FROM cdc_users WHERE _id = 2",
            )

            // Wait for Alice's update rather than counting transactions,
            // because the streaming events may batch into fewer transactions than records.
            awaitCondition("Alice updated") {
                xtQueryDb(node, "cdc", "SELECT email FROM public.cdc_users WHERE _id = 1")
                    .firstOrNull()?.get("email") == "alice-new@example.com"
            }

            // Bob: deleted — should not appear in current state
            val bob = xtQueryDb(node, "cdc",
                "SELECT _id FROM public.cdc_users WHERE _id = 2"
            )
            assertEquals(0, bob.size, "Bob should be deleted")
        }
    }

    @Test
    fun `CDC events ingested with schemas-enable=false`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS cdc_no_envelope (_id INT PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO cdc_no_envelope (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )

        registerConnectorAndAwait(
            extraConfig = mapOf(
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable" to "false",
            ),
            awaitTopics = listOf("testdb.public.cdc_no_envelope"),
        )

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachDebeziumDb(node, "testdb.public.cdc_no_envelope")

            // Phase 1: snapshot
            awaitTxs(node, 1, db = "cdc")
            val snapshotRows = xtQueryDb(node, "cdc",
                "SELECT _id, email FROM public.cdc_no_envelope ORDER BY _id"
            )
            assertEquals(1, snapshotRows.size, "Snapshot should ingest Alice")
            assertEquals("alice@example.com", snapshotRows[0]["email"])

            // Phase 2: streaming events
            pgExecute(
                "INSERT INTO cdc_no_envelope (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                "UPDATE cdc_no_envelope SET email = 'alice-new@example.com' WHERE _id = 1",
                "DELETE FROM cdc_no_envelope WHERE _id = 2",
            )

            awaitCondition("Alice updated") {
                xtQueryDb(node, "cdc", "SELECT email FROM public.cdc_no_envelope WHERE _id = 1")
                    .firstOrNull()?.get("email") == "alice-new@example.com"
            }

            val bob = xtQueryDb(node, "cdc",
                "SELECT _id FROM public.cdc_no_envelope WHERE _id = 2"
            )
            assertEquals(0, bob.size, "Bob should be deleted")
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

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        createTopic("testdb.public.timed_docs")
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachDebeziumDb(node, "testdb.public.timed_docs")

            pgExecute(
                """INSERT INTO timed_docs (_id, name, _valid_from, _valid_to)
                VALUES (1, 'bounded', '2024-01-01T00:00:00Z', '2025-01-01T00:00:00Z')""",
                """INSERT INTO timed_docs (_id, name, _valid_from)
                VALUES (2, 'from-only', '2024-06-01T00:00:00Z')""",
                """INSERT INTO timed_docs (_id, name)
                VALUES (3, 'neither')""",
            )

            awaitTxs(node, 3, db = "cdc")

            val rows = xtQueryDb(node, "cdc",
                """SELECT _id, name, _valid_from, _valid_to
                   FROM public.timed_docs
                   FOR ALL VALID_TIME
                   ORDER BY _id"""
            )

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
            assertEquals(3L, (rows[2]["_id"] as Number).toLong())
            assertEquals("neither", rows[2]["name"])
            assertTrue(rows[2]["_valid_from"] != null, "Should have system-assigned valid_from")
            assertNull(rows[2]["_valid_to"], "Should have no valid_to")
        }
    }

    @Test
    fun `_valid_to without _valid_from halts ingestion`() = runTest(timeout = 120.seconds) {
        pgExecute(
            """CREATE TABLE IF NOT EXISTS bad_valid_to (
                _id INT PRIMARY KEY,
                name TEXT,
                _valid_from TIMESTAMPTZ,
                _valid_to TIMESTAMPTZ
            )""",
            "INSERT INTO bad_valid_to (_id, name, _valid_from) VALUES (1, 'Alice', '2024-01-01T00:00:00Z')",
        )

        registerConnectorAndAwait(awaitTopics = listOf("testdb.public.bad_valid_to"))

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachDebeziumDb(node, "testdb.public.bad_valid_to")

            // Alice (valid) is ingested first
            awaitTxs(node, 1, db = "cdc")

            // Now insert a record with _valid_to but no _valid_from — halts ingestion
            pgExecute(
                """INSERT INTO bad_valid_to (_id, name, _valid_to)
                VALUES (2, 'bad', '2025-01-01T00:00:00Z')""",
            )

            assertThrows<IngestionStoppedException> {
                runBlocking { database(node, "cdc").watchers.awaitTx(Long.MAX_VALUE) }
            }

            // Alice should be ingested, bad record should not
            val rows = xtQueryDb(node, "cdc",
                "SELECT _id, name FROM public.bad_valid_to ORDER BY _id"
            )
            assertEquals(1, rows.size, "Only Alice should be ingested — bad record halted ingestion")
            assertEquals("Alice", rows[0]["name"])
        }
    }

    @Test
    fun `table without _id column halts ingestion`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS no_id_table (id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO no_id_table (id, name) VALUES (1, 'pre-existing')",
        )

        registerConnectorAndAwait(awaitTopics = listOf("testdb.public.no_id_table"))

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachDebeziumDb(node, "testdb.public.no_id_table")

            pgExecute(
                "INSERT INTO no_id_table (id, name) VALUES (2, 'also-no-_id')",
            )

            // snapshot record lacks _id — ingestion halts
            assertThrows<IngestionStoppedException> {
                runBlocking { database(node, "cdc").watchers.awaitTx(Long.MAX_VALUE) }
            }

            val rows = xtQueryDb(node, "cdc",
                "SELECT * FROM public.no_id_table FOR ALL VALID_TIME"
            )
            assertEquals(0, rows.size, "No rows should be ingested — records lack _id")
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

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        createTopic("testdb.public.typed_docs")
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachDebeziumDb(node, "testdb.public.typed_docs")

            pgExecute(
                """INSERT INTO typed_docs (_id, name, score, active, metadata, tags, notes)
                VALUES (1, 'Alice', 3.14, true, '{"city": "London", "nested": {"deep": true}}',
                        ARRAY['admin', 'user'], NULL)""",
                """INSERT INTO typed_docs (_id, name, score, active, metadata, tags, notes)
                VALUES (2, 'Bob', NULL, false, NULL, NULL, 'some notes')""",
            )

            awaitTxs(node, 2, db = "cdc")

            val rows = xtQueryDb(node, "cdc",
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

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        createTopic("testdb.inventory.products")
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachDebeziumDb(node, "testdb.inventory.products")

            pgExecute(
                "INSERT INTO inventory.products (_id, name, qty) VALUES (1, 'Widget', 100)",
            )

            awaitTxs(node, 1, db = "cdc")

            val rows = xtQueryDb(node, "cdc",
                "SELECT _id, name, qty FROM inventory.products"
            )

            assertEquals(1, rows.size)
            assertEquals(1L, (rows[0]["_id"] as Number).toLong())
            assertEquals("Widget", rows[0]["name"])
            assertEquals(100L, (rows[0]["qty"] as Number).toLong())
        }
    }

    @Test
    fun `CDC events ingested via external log (direct indexing)`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS cdc_direct (_id INT PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO cdc_direct (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )

        registerConnectorAndAwait(awaitTopics = listOf("testdb.public.cdc_direct"))

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachDebeziumDb(node, "testdb.public.cdc_direct", dbName = "cdc_direct_db")

            // Phase 1: snapshot
            awaitTxs(node, 1, db = "cdc_direct_db")
            val snapshotRows = xtQueryDb(node, "cdc_direct_db",
                "SELECT _id, email FROM public.cdc_direct ORDER BY _id"
            )
            assertEquals(1, snapshotRows.size, "Snapshot should ingest Alice")
            assertEquals("alice@example.com", snapshotRows[0]["email"])

            // Phase 2: streaming events
            pgExecute(
                "INSERT INTO cdc_direct (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                "UPDATE cdc_direct SET email = 'alice-new@example.com' WHERE _id = 1",
                "DELETE FROM cdc_direct WHERE _id = 2",
            )

            awaitCondition("Alice updated") {
                xtQueryDb(node, "cdc_direct_db", "SELECT email FROM public.cdc_direct WHERE _id = 1")
                    .firstOrNull()?.get("email") == "alice-new@example.com"
            }

            val bob = xtQueryDb(node, "cdc_direct_db",
                "SELECT _id FROM public.cdc_direct WHERE _id = 2"
            )
            assertEquals(0, bob.size, "Bob should be deleted")

            // Primary db should have no CDC data
            val primaryRows = xtQuery(node,
                "SELECT * FROM public.cdc_direct FOR ALL VALID_TIME"
            )
            assertEquals(0, primaryRows.size, "Primary database should have no CDC data")
        }
    }

    @Test
    fun `non-TIMESTAMPTZ _valid_from halts ingestion`() = runTest(timeout = 120.seconds) {
        pgExecute(
            """CREATE TABLE IF NOT EXISTS bad_times (
                _id INT PRIMARY KEY,
                name TEXT,
                _valid_from TIMESTAMP,
                _valid_to TEXT
            )""",
        )

        registerConnectorAndAwait()

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        createTopic("testdb.public.bad_times")
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachDebeziumDb(node, "testdb.public.bad_times")

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

            // invalid valid time types — ingestion halts
            assertThrows<IngestionStoppedException> {
                runBlocking { database(node, "cdc").watchers.awaitTx(Long.MAX_VALUE) }
            }

            val rows = xtQueryDb(node, "cdc",
                "SELECT * FROM public.bad_times FOR ALL VALID_TIME"
            )
            assertEquals(0, rows.size, "No rows should be ingested — both have invalid valid time")
        }
    }

    @Test
    fun `CDC consumer resumes from committed offset after restart`(@TempDir tempDir: java.nio.file.Path) = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS cdc_resume (_id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO cdc_resume (_id, name) VALUES (1, 'Alice')",
        )

        registerConnectorAndAwait(awaitTopics = listOf("testdb.public.cdc_resume"))

        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        fun openPersistentNode() = Xtdb.openNode {
            server { port = 0 }; flightSql = null
            logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", sourceTopic))
            storage(xtdb.api.storage.Storage.local(tempDir))
        }

        val cdcStoragePath = tempDir.resolve("cdc-storage")

        // First run: ingest snapshot (Alice), flush block to persist the offset token
        openPersistentNode().use { node ->
            attachDebeziumDb(node, "testdb.public.cdc_resume", storagePath = cdcStoragePath)
            awaitTxs(node, 1, db = "cdc")

            // Flush block so the external source token is persisted
            (node as Xtdb.XtdbInternal).dbCatalog.let { cat ->
                cat["cdc"]!!.sendFlushBlockMessage()
                cat.syncAll(java.time.Duration.ofSeconds(10))
            }
        }

        // Insert more data while node is down
        pgExecute(
            "INSERT INTO cdc_resume (_id, name) VALUES (2, 'Bob')",
        )

        // Second run: same storage — database is already attached from persisted config,
        // should resume from persisted token, not re-process Alice
        openPersistentNode().use { node ->
            // Wait for the node to replay the ATTACH message from the source log
            awaitDatabase(node, "cdc")

            awaitTxs(node, 2, db = "cdc")

            val rows = xtQueryDb(node, "cdc",
                "SELECT _id, name FROM public.cdc_resume ORDER BY _id"
            )
            assertEquals(2, rows.size)
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])
        }
    }
}
