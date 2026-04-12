package xtdb.debezium

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import xtdb.api.Xtdb
import xtdb.api.log.IngestionStoppedException
import xtdb.api.log.KafkaCluster
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test for Avro message format with Confluent Schema Registry.
 *
 * Produces CDC-shaped Avro records (mimicking Debezium's Avro converter output)
 * via KafkaAvroSerializer → Schema Registry → Kafka, then verifies XTDB consumes
 * them correctly with MessageFormat.Avro and KafkaAvroDeserializer.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "XTDB_SINGLE_WRITER", matches = "true")
class AvroConsumerTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092")

        private val schemaRegistry = GenericContainer("confluentinc/cp-schema-registry:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200))
            .dependsOn(kafka)

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            Startables.deepStart(kafka, schemaRegistry).join()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            schemaRegistry.stop()
            kafka.stop()
            network.close()
        }
    }

    private fun schemaRegistryUrl() =
        "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(8081)}"

    private fun createTopic(topic: String) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }
    }

    // -- Avro schemas matching Debezium's CDC envelope structure --

    private val sourceSchema = SchemaBuilder.record("Source")
        .namespace("xtdb.debezium.test")
        .fields()
        .requiredString("schema")
        .requiredString("table")
        .optionalLong("lsn")
        .endRecord()

    private fun docSchema(name: String, vararg extraFields: Pair<String, org.apache.avro.Schema>) =
        SchemaBuilder.record(name)
            .namespace("xtdb.debezium.test")
            .fields()
            .apply {
                requiredLong("_id")
                requiredString("name")
                for ((fieldName, fieldSchema) in extraFields) {
                    name(fieldName).type(fieldSchema).noDefault()
                }
            }
            .endRecord()

    private fun envelopeSchema(name: String, docSchema: org.apache.avro.Schema) =
        SchemaBuilder.record(name)
            .namespace("xtdb.debezium.test")
            .fields()
            .requiredString("op")
            .name("source").type(sourceSchema).noDefault()
            .name("before").type().unionOf().nullType().and().type(docSchema).endUnion().nullDefault()
            .name("after").type().unionOf().nullType().and().type(docSchema).endUnion().nullDefault()
            .endRecord()

    private fun cdcRecord(
        envelope: org.apache.avro.Schema,
        op: String,
        schema: String,
        table: String,
        before: GenericData.Record? = null,
        after: GenericData.Record? = null,
    ): GenericData.Record = GenericRecordBuilder(envelope)
        .set("op", op)
        .set("source", GenericRecordBuilder(sourceSchema)
            .set("schema", schema)
            .set("table", table)
            .set("lsn", 1L)
            .build())
        .set("before", before)
        .set("after", after)
        .build()

    private fun openAvroProducer(): KafkaProducer<String, Any> =
        KafkaProducer(
            mapOf(
                "bootstrap.servers" to kafka.bootstrapServers,
                "key.serializer" to StringSerializer::class.java.name,
                "value.serializer" to KafkaAvroSerializer::class.java.name,
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl(),
                "acks" to "all",
            ),
        )

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

    private fun attachAvroDebeziumDb(node: Xtdb, debeziumTopic: String, dbName: String = "cdc") {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""ATTACH DATABASE $dbName WITH $$
                    log: !Kafka
                      cluster: kafka
                      topic: test-replica-${UUID.randomUUID()}
                    externalSource: !Debezium
                      messageFormat: !Avro {}
                      log: !Kafka
                        logCluster: kafka
                        tableTopic: $debeziumTopic
                $$""")
            }
        }
    }

    private suspend fun awaitTxs(node: Xtdb, expected: Int, db: String = "cdc", timeout: Long = 15_000) {
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

    private fun database(node: Xtdb, dbName: String) =
        (node as Xtdb.XtdbInternal).dbCatalog.databaseOrNull(dbName)
            ?: throw AssertionError("Database '$dbName' does not exist")

    @Test
    fun `Avro CDC events are ingested into XTDB via Schema Registry`() = runTest(timeout = 120.seconds) {
        val topic = "testdb.public.avro_users"
        createTopic(topic)

        val userDoc = docSchema("UserValue")
        val envelope = envelopeSchema("UserEnvelope", userDoc)

        fun userRecord(id: Long, name: String) =
            GenericRecordBuilder(userDoc).set("_id", id).set("name", name).build()

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        // max.poll.records=1 ensures each CDC event is a separate transaction,
        // preserving per-event valid-time history for assertions.
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachAvroDebeziumDb(node, topic)

            openAvroProducer().use { producer ->
                // snapshot: Alice
                producer.send(ProducerRecord(topic, "1",
                    cdcRecord(envelope, "r", "public", "avro_users",
                        after = userRecord(1L, "Alice")))).get()

                // insert: Bob
                producer.send(ProducerRecord(topic, "2",
                    cdcRecord(envelope, "c", "public", "avro_users",
                        after = userRecord(2L, "Bob")))).get()

                // update: Alice
                producer.send(ProducerRecord(topic, "1",
                    cdcRecord(envelope, "u", "public", "avro_users",
                        after = userRecord(1L, "Alice-updated")))).get()

                // delete: Bob
                producer.send(ProducerRecord(topic, "2",
                    cdcRecord(envelope, "d", "public", "avro_users",
                        before = userRecord(2L, "Bob")))).get()
            }

            // snapshot(Alice) + insert(Bob) + update(Alice) + delete(Bob)
            awaitTxs(node, 4)

            val history = xtQueryDb(node, "cdc",
                """SELECT _id, name, _valid_from, _valid_to
                   FROM public.avro_users
                   FOR ALL VALID_TIME
                   ORDER BY _id, _valid_from"""
            )

            assertEquals(3, history.size, "Expected 3 history rows (2 Alice + 1 Bob)")

            // Alice: snapshot then update
            assertEquals(1L, (history[0]["_id"] as Number).toLong())
            assertEquals("Alice", history[0]["name"])
            assertTrue(history[0]["_valid_to"] != null, "Snapshot row should be superseded")

            assertEquals(1L, (history[1]["_id"] as Number).toLong())
            assertEquals("Alice-updated", history[1]["name"])
            assertNull(history[1]["_valid_to"], "Updated row should be current")

            // Bob: inserted then deleted
            assertEquals(2L, (history[2]["_id"] as Number).toLong())
            assertEquals("Bob", history[2]["name"])
            assertTrue(history[2]["_valid_to"] != null, "Bob should have valid_to from DELETE")
        }
    }

    @Test
    fun `Avro CDC multiple records ingested`() = runTest(timeout = 120.seconds) {
        val topic = "testdb.public.avro_batch"
        createTopic(topic)

        val batchDoc = docSchema("BatchValue")
        val envelope = envelopeSchema("BatchEnvelope", batchDoc)

        fun record(id: Long, name: String) =
            GenericRecordBuilder(batchDoc).set("_id", id).set("name", name).build()

        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachAvroDebeziumDb(node, topic, dbName = "avro_batch")

            openAvroProducer().use { producer ->
                producer.send(ProducerRecord(topic, "1",
                    cdcRecord(envelope, "c", "public", "avro_batch",
                        after = record(1L, "first")))).get()

                producer.send(ProducerRecord(topic, "2",
                    cdcRecord(envelope, "c", "public", "avro_batch",
                        after = record(2L, "second")))).get()
            }

            awaitTxs(node, 1, db = "avro_batch")

            val rows = xtQueryDb(node, "avro_batch",
                "SELECT _id, name FROM public.avro_batch ORDER BY _id"
            )

            assertEquals(2, rows.size)
            assertEquals("first", rows[0]["name"])
            assertEquals("second", rows[1]["name"])
        }
    }

    @Test
    fun `Avro CDC with additional column types`() = runTest(timeout = 120.seconds) {
        val topic = "testdb.public.avro_typed"
        createTopic(topic)

        val typedDoc = docSchema("TypedValue",
            "score" to SchemaBuilder.builder().doubleType(),
            "active" to SchemaBuilder.builder().booleanType(),
        )
        val envelope = envelopeSchema("TypedEnvelope", typedDoc)

        val doc = GenericRecordBuilder(typedDoc)
            .set("_id", 1L)
            .set("name", "Alice")
            .set("score", 3.14)
            .set("active", true)
            .build()

        openAvroProducer().use { producer ->
            producer.send(ProducerRecord(topic, "1",
                cdcRecord(envelope, "c", "public", "avro_typed", after = doc))).get()
        }

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachAvroDebeziumDb(node, topic, dbName = "avro_typed")

            awaitTxs(node, 1, db = "avro_typed")

            val rows = xtQueryDb(node, "avro_typed",
                "SELECT _id, name, score, active FROM public.avro_typed"
            )

            assertEquals(1, rows.size)
            assertEquals(1L, (rows[0]["_id"] as Number).toLong())
            assertEquals("Alice", rows[0]["name"])
            assertEquals(3.14, (rows[0]["score"] as Number).toDouble(), 0.001)
            assertEquals(true, rows[0]["active"])
        }
    }

    // -- Error case tests: verify Avro-decoded maps trigger the same errors as JSON --

    @Test
    fun `Avro CDC without _id halts ingestion`() = runTest(timeout = 120.seconds) {
        val topic = "testdb.public.avro_no_id"
        createTopic(topic)

        // Doc schema without _id — just has "id" (wrong name)
        val noIdDoc = SchemaBuilder.record("NoIdValue")
            .namespace("xtdb.debezium.test")
            .fields()
            .requiredLong("id")
            .requiredString("name")
            .endRecord()
        val envelope = envelopeSchema("NoIdEnvelope", noIdDoc)

        val doc = GenericRecordBuilder(noIdDoc)
            .set("id", 1L)
            .set("name", "Alice")
            .build()

        openAvroProducer().use { producer ->
            producer.send(ProducerRecord(topic, "1",
                cdcRecord(envelope, "c", "public", "avro_no_id", after = doc))).get()
        }

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic).use { node ->
            attachAvroDebeziumDb(node, topic, dbName = "avro_no_id")

            assertThrows<IngestionStoppedException> {
                runBlocking { database(node, "avro_no_id").watchers.awaitTx(Long.MAX_VALUE) }
            }

            val rows = xtQueryDb(node, "avro_no_id",
                "SELECT * FROM public.avro_no_id FOR ALL VALID_TIME"
            )
            assertEquals(0, rows.size, "No rows should be ingested — records lack _id")
        }
    }

    @Test
    fun `Avro CDC with _valid_to but no _valid_from halts ingestion`() = runTest(timeout = 120.seconds) {
        val topic = "testdb.public.avro_bad_vt"
        createTopic(topic)

        val vtDoc = SchemaBuilder.record("BadVtValue")
            .namespace("xtdb.debezium.test")
            .fields()
            .requiredLong("_id")
            .requiredString("name")
            .name("_valid_from").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("_valid_to").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .endRecord()
        val envelope = envelopeSchema("BadVtEnvelope", vtDoc)

        // First: a valid record so the database gets created
        val validDoc = GenericRecordBuilder(vtDoc)
            .set("_id", 1L)
            .set("name", "valid")
            .set("_valid_from", "2024-01-01T00:00:00Z")
            .set("_valid_to", null)
            .build()

        // Second: _valid_to without _valid_from — should halt
        val badDoc = GenericRecordBuilder(vtDoc)
            .set("_id", 2L)
            .set("name", "bad")
            .set("_valid_from", null)
            .set("_valid_to", "2025-01-01T00:00:00Z")
            .build()

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachAvroDebeziumDb(node, topic, dbName = "avro_bad_vt")

            openAvroProducer().use { producer ->
                producer.send(ProducerRecord(topic, "1",
                    cdcRecord(envelope, "c", "public", "avro_bad_vt", after = validDoc))).get()
                producer.send(ProducerRecord(topic, "2",
                    cdcRecord(envelope, "c", "public", "avro_bad_vt", after = badDoc))).get()
            }

            // First record ingested OK
            awaitTxs(node, 1, db = "avro_bad_vt")

            // Second record halts ingestion
            assertThrows<IngestionStoppedException> {
                runBlocking { database(node, "avro_bad_vt").watchers.awaitTx(Long.MAX_VALUE) }
            }

            val rows = xtQueryDb(node, "avro_bad_vt",
                "SELECT _id, name FROM public.avro_bad_vt ORDER BY _id"
            )
            assertEquals(1, rows.size, "Only valid record should be ingested")
            assertEquals("valid", rows[0]["name"])
        }
    }

    // -- Valid time tests: verify Avro-decoded _valid_from/_valid_to are honoured --

    @Test
    fun `Avro CDC with valid time bounds`() = runTest(timeout = 120.seconds) {
        val topic = "testdb.public.avro_vt"
        createTopic(topic)

        val vtDoc = SchemaBuilder.record("VtValue")
            .namespace("xtdb.debezium.test")
            .fields()
            .requiredLong("_id")
            .requiredString("name")
            .name("_valid_from").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("_valid_to").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .endRecord()
        val envelope = envelopeSchema("VtEnvelope", vtDoc)

        // Both bounds set
        val bounded = GenericRecordBuilder(vtDoc)
            .set("_id", 1L).set("name", "bounded")
            .set("_valid_from", "2024-01-01T00:00:00Z")
            .set("_valid_to", "2025-01-01T00:00:00Z")
            .build()

        // Only _valid_from
        val fromOnly = GenericRecordBuilder(vtDoc)
            .set("_id", 2L).set("name", "from-only")
            .set("_valid_from", "2024-06-01T00:00:00Z")
            .set("_valid_to", null)
            .build()

        // Neither — system-assigned
        val neither = GenericRecordBuilder(vtDoc)
            .set("_id", 3L).set("name", "neither")
            .set("_valid_from", null)
            .set("_valid_to", null)
            .build()

        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        openNodeOnSourceTopic(sourceTopic, mapOf("max.poll.records" to "1")).use { node ->
            attachAvroDebeziumDb(node, topic, dbName = "avro_vt")

            openAvroProducer().use { producer ->
                producer.send(ProducerRecord(topic, "1",
                    cdcRecord(envelope, "c", "public", "avro_vt", after = bounded))).get()
                producer.send(ProducerRecord(topic, "2",
                    cdcRecord(envelope, "c", "public", "avro_vt", after = fromOnly))).get()
                producer.send(ProducerRecord(topic, "3",
                    cdcRecord(envelope, "c", "public", "avro_vt", after = neither))).get()
            }

            awaitTxs(node, 3, db = "avro_vt")

            val rows = xtQueryDb(node, "avro_vt",
                """SELECT _id, name, _valid_from, _valid_to
                   FROM public.avro_vt
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
}
