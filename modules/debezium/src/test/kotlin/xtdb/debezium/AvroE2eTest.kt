package xtdb.debezium

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.DriverManager
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * End-to-end integration test for the full Avro CDC pipeline:
 * PostgreSQL → Debezium Connect (Avro converter) → Schema Registry → Kafka → XTDB
 *
 * Uses Confluent's cp-kafka-connect image (which includes the Avro converter)
 * with the Debezium PostgreSQL connector installed via Maven archive.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "XTDB_SINGLE_WRITER", matches = "true")
class AvroE2eTest {

    private lateinit var network: Network
    private lateinit var postgres: PostgreSQLContainer
    private lateinit var kafka: ConfluentKafkaContainer
    private lateinit var schemaRegistry: GenericContainer<*>
    private lateinit var debeziumConnect: GenericContainer<*>

    private val httpClient: HttpClient = HttpClient.newHttpClient()

    companion object {
        // Confluent Connect image with Debezium PostgreSQL connector added.
        // Built once and cached by Docker for subsequent runs.
        private val connectImage = ImageFromDockerfile()
            .withDockerfileFromBuilder { builder ->
                builder
                    .from("confluentinc/cp-kafka-connect:7.8.0")
                    .run("mkdir -p /usr/share/confluent-hub-components/debezium-postgresql && cd /usr/share/confluent-hub-components/debezium-postgresql && curl -sL 'https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.0.2.Final/debezium-connector-postgres-3.0.2.Final-plugin.tar.gz' | tar xz")
                    .build()
            }
    }

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

        schemaRegistry = GenericContainer("confluentinc/cp-schema-registry:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200))
            .dependsOn(kafka)

        debeziumConnect = GenericContainer(connectImage)
            .withNetwork(network)
            .withExposedPorts(8083)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("CONNECT_REST_PORT", "8083")
            .withEnv("CONNECT_GROUP_ID", "debezium-avro-connect")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "debezium_avro_configs")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "debezium_avro_offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "debezium_avro_statuses")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
            .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "debezium-connect")
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components")
            // Internal converter for config/offset/status topics
            .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200))
            .dependsOn(kafka, schemaRegistry)

        Startables.deepStart(postgres, kafka, schemaRegistry, debeziumConnect).join()
    }

    @AfterEach
    fun tearDown() {
        debeziumConnect.stop()
        schemaRegistry.stop()
        kafka.stop()
        postgres.stop()
        network.close()
    }

    private fun schemaRegistryUrl() =
        "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(8081)}"

    private fun connectUrl() =
        "http://${debeziumConnect.host}:${debeziumConnect.getMappedPort(8083)}"

    private suspend fun registerConnectorAndAwait(
        schemas: String = "public",
        extraConfig: Map<String, String> = emptyMap(),
    ) {
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
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers)
            .schemaRegistryUrl(schemaRegistryUrl())
            .propertiesMap(kafkaProperties))
        log(KafkaCluster.LogFactory("kafka", sourceTopic))
    }

    private fun attachAvroDebeziumDb(
        node: Xtdb,
        debeziumTopic: String,
        dbName: String = "cdc",
    ) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""ATTACH DATABASE $dbName WITH $$
                    log: !Kafka
                      cluster: kafka
                      topic: test-replica-${UUID.randomUUID()}
                    externalLog: !Debezium
                      messageFormat: !Avro {}
                      log: !Kafka
                        logCluster: kafka
                        tableTopic: $debeziumTopic
                $$""")
            }
        }
    }

    private suspend fun awaitTxs(node: Xtdb, expected: Int, db: String = "cdc", timeout: Long = 30_000) {
        val deadline = System.currentTimeMillis() + timeout
        var count = 0L
        while (System.currentTimeMillis() < deadline) {
            count = xtQueryDb(node, db, "SELECT count(*) AS cnt FROM xt.txs")[0]["cnt"] as Long
            if (count >= expected) return
            delay(200)
        }
        throw AssertionError("Timed out waiting for $expected txs on db '$db' (got $count)")
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

    @Test
    fun `Avro CDC from Debezium Connect ingested into XTDB`() = runTest(timeout = 120.seconds) {
        pgExecute(
            "CREATE TABLE IF NOT EXISTS cdc_avro (_id INT PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO cdc_avro (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )

        registerConnectorAndAwait(extraConfig = mapOf(
            "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
            "value.converter" to "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url" to "http://schema-registry:8081",
        ))

        pgExecute(
            "INSERT INTO cdc_avro (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
            "UPDATE cdc_avro SET email = 'alice-new@example.com' WHERE _id = 1",
            "DELETE FROM cdc_avro WHERE _id = 2",
        )

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachAvroDebeziumDb(node, "testdb.public.cdc_avro")

            // snapshot(Alice) + insert(Bob) + update(Alice) + delete(Bob)
            awaitTxs(node, 4)

            val history = xtQueryDb(node, "cdc",
                """SELECT _id, name, email, _valid_from, _valid_to
                   FROM public.cdc_avro
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

            // Bob: inserted then deleted
            assertEquals(2L, (history[2]["_id"] as Number).toLong())
            assertEquals("bob@example.com", history[2]["email"])
            assertTrue(history[2]["_valid_to"] != null, "Bob should have valid_to from DELETE")
        }
    }
}
