package xtdb.kafkaconnectsource

import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaConnectSourceIntegrationTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")

        private val schemaRegistry: GenericContainer<*> = GenericContainer("confluentinc/cp-schema-registry:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .dependsOn(kafka)

        private val schemaRegistryUrl: String
            get() = "http://${schemaRegistry.host}:${schemaRegistry.firstMappedPort}"

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

    private fun createTopic(topic: String) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }
    }

    private fun openNode(sourceTopic: String): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
        log(KafkaCluster.LogFactory("kafka", sourceTopic))
    }

    private fun attach(node: Xtdb, dbName: String, yaml: String) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("ATTACH DATABASE $dbName WITH \$\$\n$yaml\n\$\$")
            }
        }
    }

    private fun produceBytes(topic: String, key: String?, value: ByteArray?) {
        val producerProps = mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to StringSerializer::class.java.name,
            "value.serializer" to ByteArraySerializer::class.java.name,
        )
        KafkaProducer<String?, ByteArray?>(producerProps).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get()
        }
    }

    private fun produceAvro(topic: String, key: String?, value: GenericData.Record?) {
        val producerProps = mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to StringSerializer::class.java.name,
            "value.serializer" to "io.confluent.kafka.serializers.KafkaAvroSerializer",
            "schema.registry.url" to schemaRegistryUrl,
        )
        KafkaProducer<String?, GenericData.Record?>(producerProps).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get()
        }
    }

    private fun xtQueryDb(node: Xtdb, dbName: String, sql: String): List<Map<String, Any?>> =
        node.createConnectionBuilder().database(dbName).build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    val md = rs.metaData
                    val cols = (1..md.columnCount).map { md.getColumnName(it) }
                    buildList {
                        while (rs.next()) add(cols.associateWith { rs.getObject(it) })
                    }
                }
            }
        }

    private suspend fun awaitCondition(description: String, timeout: Duration = 30.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (System.currentTimeMillis() < deadline) {
            if (runCatching(check).getOrDefault(false)) return
            runInterruptible { Thread.sleep(200) }
        }
        throw AssertionError("Timed out waiting for: $description")
    }

    @Test
    fun `JsonConverter happy path snapshot + streaming`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceBytes(sourceTopic, "k1", """{"name":"Alice","age":30}""".toByteArray())
        produceBytes(sourceTopic, "k2", """{"_id":"explicit","name":"Bob"}""".toByteArray())

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("both records appear") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events").size == 2
            }

            val k1 = xtQueryDb(node, "events", "SELECT _id, name, age FROM public.events WHERE _id = 'k1'")
            assertEquals(1, k1.size)
            assertEquals("Alice", k1[0]["name"])

            val k2 = xtQueryDb(node, "events", "SELECT _id, name FROM public.events WHERE _id = 'explicit'")
            assertEquals(1, k2.size)
            assertEquals("Bob", k2[0]["name"])

            produceBytes(sourceTopic, "k3", """{"name":"Charlie"}""".toByteArray())
            awaitCondition("streamed record appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k3'").isNotEmpty()
            }
        }
    }

    @Test
    fun `null value tombstone deletes the row (JsonConverter)`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceBytes(sourceTopic, "k1", """{"name":"Alice"}""".toByteArray())

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("Alice ingested") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isNotEmpty()
            }

            produceBytes(sourceTopic, "k1", null)
            awaitCondition("Alice deleted") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isEmpty()
            }
        }
    }

    @Test
    fun `resume from token after restart`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        val xtLog = "xt-log-${UUID.randomUUID()}"
        val replicaTopic = "test-replica-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        val attachYaml = """
            log: !Kafka
              cluster: kafka
              topic: $replicaTopic
            externalSource: !KafkaConnect
              remote: kafka
              topic: $sourceTopic
              connectConfig:
                key.converter: org.apache.kafka.connect.storage.StringConverter
                value.converter: org.apache.kafka.connect.json.JsonConverter
                value.converter.schemas.enable: "false"
              indexer: !Docs
                table: events
        """.trimIndent()

        produceBytes(sourceTopic, "a", """{"name":"Alice"}""".toByteArray())

        openNode(xtLog).use { node ->
            attach(node, "events", attachYaml)

            awaitCondition("Alice appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'a'").isNotEmpty()
            }

            produceBytes(sourceTopic, "b", """{"name":"Bob"}""".toByteArray())
            awaitCondition("Bob appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'b'").isNotEmpty()
            }
        }

        produceBytes(sourceTopic, "c", """{"name":"Charlie"}""".toByteArray())

        openNode(xtLog).use { node ->
            awaitCondition("Charlie appears after restart", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'c'").isNotEmpty()
            }

            val rows = xtQueryDb(node, "events", "SELECT _id, name FROM public.events ORDER BY _id")
            assertEquals(3, rows.size, "All three rows present — no duplication, no loss")
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])
            assertEquals("Charlie", rows[2]["name"])
        }
    }

    @Test
    fun `deserialization failure halts the source`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Prime the topic with a good record so we can confirm the source is healthy
        // before we feed it a bad one.
        produceBytes(sourceTopic, "first", """{"name":"First"}""".toByteArray())

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("first record ingested — source is healthy") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'first'").isNotEmpty()
            }

            // Now a bad record followed by a good one. Under halt-on-decode-failure, the
            // source dies on the bad record; the trailing good record is never indexed.
            produceBytes(sourceTopic, "bad", "{not-json".toByteArray())
            produceBytes(sourceTopic, "second", """{"name":"Second"}""".toByteArray())

            // Give the source time to crash on the bad record. Brittle, but no API for
            // observing "source died".
            runInterruptible { Thread.sleep(5_000) }

            val second = xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'second'")
            assertTrue(second.isEmpty(), "second shouldn't appear — source halts at the bad record under halt-on-decode-failure")

            // No DLQ-style aborted tx — halt-on-failure means decode errors propagate
            // rather than getting recorded as aborted txs.
            val aborted = xtQueryDb(node, "events", "SELECT _id FROM xt.txs WHERE committed = false")
            assertTrue(aborted.isEmpty(), "no aborted txs under halt-on-decode-failure, got: $aborted")
        }
    }

    @Test
    fun `SMT chain applies ExtractField to unwrap envelope`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceBytes(
            sourceTopic, "k1",
            """{"op":"c","payload":{"_id":"alice","name":"Alice","age":30}}""".toByteArray(),
        )

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                    transforms: unwrap
                    transforms.unwrap.type: org.apache.kafka.connect.transforms.ExtractField${'$'}Value
                    transforms.unwrap.field: payload
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("unwrapped record appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'alice'").isNotEmpty()
            }

            val rows = xtQueryDb(node, "events", "SELECT _id, name, age FROM public.events WHERE _id = 'alice'")
            assertEquals("Alice", rows[0]["name"])
            // the envelope's `op` field should not be on the unwrapped doc - assert via the catalog,
            // since referencing a column that doesn't exist is now an error rather than an empty result
            assertTrue(
                xtQueryDb(node, "events",
                          "SELECT column_name FROM information_schema.columns WHERE table_name = 'events' AND column_name = 'op'").isEmpty(),
                "envelope's op field should not be on the unwrapped doc",
            )
        }
    }

    @Test
    fun `SMT chain runs transforms in declaration order`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Envelope-shaped: { meta, payload: { _id, name, email } }
        // Chain: unwrap (ExtractField -> payload) THEN mask (MaskField -> email).
        // Order matters: mask must run on the unwrapped payload, not the outer envelope.
        produceBytes(
            sourceTopic, "k1",
            """{"meta":{"op":"c"},"payload":{"_id":"alice","name":"Alice","email":"a@example.com"}}""".toByteArray(),
        )

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                    transforms: unwrap,mask
                    transforms.unwrap.type: org.apache.kafka.connect.transforms.ExtractField${'$'}Value
                    transforms.unwrap.field: payload
                    transforms.mask.type: org.apache.kafka.connect.transforms.MaskField${'$'}Value
                    transforms.mask.fields: email
                    transforms.mask.replacement: REDACTED
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("record appears with both transforms applied") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'alice'").isNotEmpty()
            }

            val rows = xtQueryDb(node, "events", "SELECT _id, name, email FROM public.events WHERE _id = 'alice'")
            assertEquals("Alice", rows[0]["name"], "name should pass through")
            assertEquals("REDACTED", rows[0]["email"], "email should be masked by the second transform")
        }
    }

    @Test
    fun `predicate with negate skips tombstones from a transform that would crash on null`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Two records, distinct keys, so we can observe each independently:
        //   - alice: regular record — predicate.test=false on this, negate→true, mask APPLIES,
        //                             email should land masked.
        //   - bob:   tombstone     — predicate.test=true on this, negate→false, mask SKIPPED,
        //                             record reaches indexer unmolested (no aborted tx, no row
        //                             since there's nothing to delete).
        produceBytes(sourceTopic, "alice", """{"_id":"alice","email":"a@example.com"}""".toByteArray())
        produceBytes(sourceTopic, "bob", null)

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                    transforms: mask
                    transforms.mask.type: org.apache.kafka.connect.transforms.MaskField${'$'}Value
                    transforms.mask.fields: email
                    transforms.mask.replacement: REDACTED
                    transforms.mask.predicate: notTombstone
                    transforms.mask.negate: "true"
                    predicates: notTombstone
                    predicates.notTombstone.type: org.apache.kafka.connect.transforms.predicates.RecordIsTombstone
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("Alice ingested with masked email") {
                val rows = xtQueryDb(node, "events", "SELECT email FROM public.events WHERE _id = 'alice'")
                rows.isNotEmpty() && rows[0]["email"] == "REDACTED"
            }

            // Without the predicate, MaskField would crash on the tombstone's null value
            // and we'd see an aborted tx for that record.
            val aborted = xtQueryDb(node, "events", "SELECT committed FROM xt.txs WHERE committed = false")
            assertTrue(aborted.isEmpty(), "no aborted txs — tombstone should bypass MaskField via the predicate, got: $aborted")
        }
    }

    // Helpers that paper over JDBC's "could be any of these" behaviour for date / time / timestamp.
    private fun toInstant(v: Any?): java.time.Instant = when (v) {
        is java.time.Instant -> v
        is java.time.ZonedDateTime -> v.toInstant()
        is java.time.OffsetDateTime -> v.toInstant()
        is java.sql.Timestamp -> v.toInstant()
        else -> error("unexpected timestamp type: ${v?.javaClass}")
    }

    private fun toLocalDate(v: Any?): java.time.LocalDate = when (v) {
        is java.time.LocalDate -> v
        is java.sql.Date -> v.toLocalDate()
        is String -> java.time.LocalDate.parse(v)
        else -> error("unexpected date type: ${v?.javaClass}")
    }

    private fun toLocalTime(v: Any?): java.time.LocalTime = when (v) {
        is java.time.LocalTime -> v
        is java.sql.Time -> v.toLocalTime()
        is String -> java.time.LocalTime.parse(v)
        else -> error("unexpected time type: ${v?.javaClass}")
    }

    /** JDBC returns arrays as `java.sql.Array` (PostgreSQL's `PgArray`); unwrap to a plain List. */
    private fun toList(v: Any?): List<Any?> = when (v) {
        is List<*> -> v
        is java.sql.Array -> (v.array as Array<*>).toList()
        else -> error("unexpected array type: ${v?.javaClass}")
    }

    @Test
    fun `JsonConverter roundtrips all JSON types`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // JSON's native types: string, integer, number (double), boolean, null, array, object.
        produceBytes(
            sourceTopic, "all-types",
            """
            {
              "_id": "all-types",
              "string_val": "hello",
              "int_val": 42,
              "long_val": 9999999999,
              "double_val": 3.14,
              "bool_val": true,
              "null_val": null,
              "array_val": [1, 2, 3],
              "nested_val": {"role": "admin", "level": 5}
            }
            """.trimIndent().toByteArray(),
        )

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                    value.converter.schemas.enable: "false"
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("record appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'all-types'").isNotEmpty()
            }

            val row = xtQueryDb(
                node, "events",
                "SELECT _id, string_val, int_val, long_val, double_val, bool_val, null_val, array_val, nested_val " +
                    "FROM public.events WHERE _id = 'all-types'",
            )[0]

            assertEquals("hello", row["string_val"])
            assertEquals(42L, (row["int_val"] as Number).toLong())
            assertEquals(9999999999L, (row["long_val"] as Number).toLong())
            assertEquals(3.14, (row["double_val"] as Number).toDouble(), 0.0001)
            assertEquals(true, row["bool_val"])
            assertEquals(null, row["null_val"])

            assertEquals(listOf(1L, 2L, 3L), toList(row["array_val"]).map { (it as Number).toLong() })

            @Suppress("UNCHECKED_CAST")
            val nested = row["nested_val"] as Map<String, Any?>
            assertEquals("admin", nested["role"])
            assertEquals(5L, (nested["level"] as Number).toLong())
        }
    }

    @Test
    fun `JsonSchemaConverter with Schema Registry roundtrips all JSON Schema types`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // The schema covers two paths through the converter:
        //   1. Native JSON Schema types (string / integer / number / boolean / array / object).
        //   2. Connect logical types via `title` — `type: integer` + `title` set to the Connect
        //      logical-type name is JsonSchemaConverter's supported way to ingest typed
        //      Timestamp/Date/Time. (Plain `format: date-time` etc. without `title` is metadata
        //      only and arrives as a string — see the `*_str` fields below for that path.)
        val schemaStr = """
            {
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "type": "object",
              "properties": {
                "_id":           { "type": "string" },
                "string_val":    { "type": "string" },
                "int_val":       { "type": "integer" },
                "double_val":    { "type": "number" },
                "bool_val":      { "type": "boolean" },
                "array_val":     { "type": "array", "items": { "type": "integer" } },
                "nested_val":    {
                  "type": "object",
                  "properties": {
                    "inner": { "type": "string" }
                  }
                },
                "timestamp_str": { "type": "string", "format": "date-time" },
                "date_str":      { "type": "string", "format": "date" },
                "time_str":      { "type": "string", "format": "time" },
                "timestamp_val": { "type": "integer", "title": "org.apache.kafka.connect.data.Timestamp" },
                "date_val":      { "type": "integer", "title": "org.apache.kafka.connect.data.Date" },
                "time_val":      { "type": "integer", "title": "org.apache.kafka.connect.data.Time" }
              }
            }
        """.trimIndent()

        val mapper = com.fasterxml.jackson.databind.ObjectMapper()

        // `*_str` fields: documents the format-metadata-only behaviour (arrives as strings).
        val timestampStr = "2024-06-08T12:34:56.789Z"
        val dateStr = "2024-06-08"
        // JSON Schema's `time` format requires a zone designator (RFC 3339 full-time).
        val timeStr = "12:34:56Z"

        // `*_val` fields: typed temporals via `title` + integer. Producer emits Connect's wire
        // encoding for each logical type: Timestamp → millis since epoch, Date → days since epoch,
        // Time → millis since midnight.
        val expectedTs = java.time.Instant.parse("2024-06-08T12:34:56.789Z")
        val expectedDate = java.time.LocalDate.of(2024, 6, 8)
        val expectedTime = java.time.LocalTime.of(12, 34, 56)
        val tsMillis = expectedTs.toEpochMilli()
        val daysSinceEpoch = expectedDate.toEpochDay()
        val millisOfDay = expectedTime.toNanoOfDay() / 1_000_000L

        val payloadJson = mapper.readTree("""
            {
              "_id": "all-types",
              "string_val": "hello",
              "int_val": 42,
              "double_val": 3.14,
              "bool_val": true,
              "array_val": [1, 2, 3],
              "nested_val": {"inner": "deep"},
              "timestamp_str": "$timestampStr",
              "date_str": "$dateStr",
              "time_str": "$timeStr",
              "timestamp_val": $tsMillis,
              "date_val": $daysSinceEpoch,
              "time_val": $millisOfDay
            }
        """.trimIndent())

        val jsonSchema = io.confluent.kafka.schemaregistry.json.JsonSchema(schemaStr)
        val envelope = io.confluent.kafka.schemaregistry.json.JsonSchemaUtils.toObject(payloadJson, jsonSchema)

        val producerProps = mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to StringSerializer::class.java.name,
            "value.serializer" to "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
            "schema.registry.url" to schemaRegistryUrl,
            "auto.register.schemas" to "true",
        )
        KafkaProducer<String?, Any?>(producerProps).use { producer ->
            producer.send(ProducerRecord(sourceTopic, "all-types", envelope)).get()
        }

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: io.confluent.connect.json.JsonSchemaConverter
                    value.converter.schema.registry.url: $schemaRegistryUrl
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("record appears", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'all-types'").isNotEmpty()
            }

            val row = xtQueryDb(
                node, "events",
                "SELECT _id, string_val, int_val, double_val, bool_val, array_val, nested_val, " +
                    "timestamp_str, date_str, time_str, " +
                    "timestamp_val, date_val, time_val " +
                    "FROM public.events WHERE _id = 'all-types'",
            )[0]

            assertEquals("hello", row["string_val"])
            assertEquals(42L, (row["int_val"] as Number).toLong())
            assertEquals(3.14, (row["double_val"] as Number).toDouble(), 0.0001)
            assertEquals(true, row["bool_val"])

            assertEquals(listOf(1L, 2L, 3L), toList(row["array_val"]).map { (it as Number).toLong() })

            @Suppress("UNCHECKED_CAST")
            val nested = row["nested_val"] as Map<String, Any?>
            assertEquals("deep", nested["inner"])

            // `*_str` fields — JSON Schema's `format` keyword (date-time / date / time) is metadata
            // only. Confluent's JsonSchemaConverter doesn't promote it to a Connect logical type;
            // the values arrive as strings and XT stores them as strings. If you need typed temporals
            // and can't change the producer, drop in a `TimestampConverter` SMT.
            assertEquals(timestampStr, row["timestamp_str"])
            assertEquals(dateStr, row["date_str"])
            assertEquals(timeStr, row["time_str"])

            // `*_val` fields — typed temporals via `title` + integer. The field is `type: integer`
            // with `title` pointing at the Connect logical-type name, and the producer emits Connect's
            // integer wire encoding. JsonSchemaConverter promotes to a Connect logical type and the
            // values come back as typed temporals.
            assertEquals(expectedTs, toInstant(row["timestamp_val"]))
            assertEquals(expectedDate, toLocalDate(row["date_val"]))
            assertEquals(expectedTime, toLocalTime(row["time_val"]))
        }
    }

    @Test
    fun `null value tombstone deletes the row (JsonSchemaConverter)`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        val schemaStr = """
            {
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "type": "object",
              "properties": {
                "_id":  { "type": "string" },
                "name": { "type": "string" }
              }
            }
        """.trimIndent()

        val mapper = com.fasterxml.jackson.databind.ObjectMapper()
        val jsonSchema = io.confluent.kafka.schemaregistry.json.JsonSchema(schemaStr)
        val envelope = io.confluent.kafka.schemaregistry.json.JsonSchemaUtils.toObject(
            mapper.readTree("""{"_id":"k1","name":"Alice"}"""),
            jsonSchema,
        )

        val producerProps = mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to StringSerializer::class.java.name,
            "value.serializer" to "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
            "schema.registry.url" to schemaRegistryUrl,
            "auto.register.schemas" to "true",
        )
        KafkaProducer<String?, Any?>(producerProps).use { producer ->
            producer.send(ProducerRecord(sourceTopic, "k1", envelope)).get()
        }

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: io.confluent.connect.json.JsonSchemaConverter
                    value.converter.schema.registry.url: $schemaRegistryUrl
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("Alice ingested", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isNotEmpty()
            }

            // Tombstones are just null value bytes — same wire shape regardless of format.
            produceBytes(sourceTopic, "k1", null)
            awaitCondition("Alice deleted") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isEmpty()
            }
        }
    }

    @Test
    fun `null value tombstone deletes the row (AvroConverter)`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        val avroSchema = AvroSchema.Parser().parse("""
            {
              "type":"record","name":"Person","namespace":"xtdb.test",
              "fields":[
                {"name":"_id","type":"string"},
                {"name":"name","type":"string"}
              ]
            }
        """.trimIndent())

        val rec = GenericData.Record(avroSchema).apply {
            put("_id", "k1")
            put("name", "Alice")
        }
        produceAvro(sourceTopic, "k1", rec)

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: io.confluent.connect.avro.AvroConverter
                    value.converter.schema.registry.url: $schemaRegistryUrl
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("Alice ingested", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isNotEmpty()
            }

            produceBytes(sourceTopic, "k1", null)
            awaitCondition("Alice deleted") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isEmpty()
            }
        }
    }

    @Test
    fun `AvroConverter recursively converts logical types inside nested compound shapes`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Logical types at various depths:
        //   - Struct → Struct → Decimal + Timestamp
        //   - Struct → Array<Timestamp>
        //   - Struct → Map<String, Struct<Decimal>>
        // The schema-driven walk has to propagate logical-type recognition through every level.
        val schemaJson = """
            {
              "type": "record", "name": "OrderEnvelope", "namespace": "xtdb.test",
              "fields": [
                {"name": "_id", "type": "string"},
                {"name": "header", "type": {
                  "type": "record", "name": "Header",
                  "fields": [
                    {"name": "placed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "totals", "type": {
                      "type": "record", "name": "Totals",
                      "fields": [
                        {"name": "subtotal", "type": {"type": "bytes", "logicalType": "decimal",
                                                     "precision": 12, "scale": 2}}
                      ]
                    }}
                  ]
                }},
                {"name": "delivery_attempts", "type": {
                  "type": "array",
                  "items": {"type": "long", "logicalType": "timestamp-millis"}
                }},
                {"name": "lines_by_sku", "type": {
                  "type": "map",
                  "values": {
                    "type": "record", "name": "Line",
                    "fields": [
                      {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal",
                                                  "precision": 12, "scale": 2}}
                    ]
                  }
                }}
              ]
            }
        """.trimIndent()
        val avroSchema = AvroSchema.Parser().parse(schemaJson)

        val placedAt = java.time.Instant.parse("2024-06-08T12:34:56.789Z")
        val attempt1 = java.time.Instant.parse("2024-06-08T14:00:00Z")
        val attempt2 = java.time.Instant.parse("2024-06-09T10:00:00Z")
        val subtotal = java.math.BigDecimal("999.99")
        val lineA = java.math.BigDecimal("100.00")
        val lineB = java.math.BigDecimal("250.50")

        val decimalConv = org.apache.avro.Conversions.DecimalConversion()
        fun decimalBytes(value: java.math.BigDecimal, fieldName: String, parent: AvroSchema): java.nio.ByteBuffer {
            val fieldSchema = parent.getField(fieldName).schema()
            return decimalConv.toBytes(value, fieldSchema, fieldSchema.logicalType)
        }

        val totalsSchema = avroSchema.getField("header").schema().getField("totals").schema()
        val totalsRec = GenericData.Record(totalsSchema).apply {
            put("subtotal", decimalBytes(subtotal, "subtotal", totalsSchema))
        }

        val headerSchema = avroSchema.getField("header").schema()
        val headerRec = GenericData.Record(headerSchema).apply {
            put("placed_at", placedAt.toEpochMilli())
            put("totals", totalsRec)
        }

        val lineSchema = avroSchema.getField("lines_by_sku").schema().valueType
        fun lineRec(amt: java.math.BigDecimal) = GenericData.Record(lineSchema).apply {
            put("amount", decimalBytes(amt, "amount", lineSchema))
        }

        val envelope = GenericData.Record(avroSchema).apply {
            put("_id", "nested")
            put("header", headerRec)
            put("delivery_attempts", listOf(attempt1.toEpochMilli(), attempt2.toEpochMilli()))
            put("lines_by_sku", mapOf("SKU-A" to lineRec(lineA), "SKU-B" to lineRec(lineB)))
        }

        produceAvro(sourceTopic, "nested", envelope)

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: io.confluent.connect.avro.AvroConverter
                    value.converter.schema.registry.url: $schemaRegistryUrl
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("record appears", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'nested'").isNotEmpty()
            }

            val row = xtQueryDb(
                node, "events",
                "SELECT _id, header, delivery_attempts, lines_by_sku FROM public.events WHERE _id = 'nested'",
            )[0]

            // header.placed_at: Timestamp two structs deep.
            // header.totals.subtotal: Decimal three structs deep.
            @Suppress("UNCHECKED_CAST")
            val header = row["header"] as Map<String, Any?>
            assertEquals(placedAt, toInstant(header["placed_at"]))

            @Suppress("UNCHECKED_CAST")
            val totals = header["totals"] as Map<String, Any?>
            assertEquals(subtotal, totals["subtotal"])

            // delivery_attempts: Array<Timestamp> — every element should be an Instant, not a Long.
            val attempts = toList(row["delivery_attempts"])
            assertEquals(2, attempts.size)
            assertEquals(attempt1, toInstant(attempts[0]))
            assertEquals(attempt2, toInstant(attempts[1]))

            // lines_by_sku: Map<String, Struct<Decimal>> — Decimal at array-of-struct depth.
            @Suppress("UNCHECKED_CAST")
            val lines = row["lines_by_sku"] as Map<String, Any?>
            @Suppress("UNCHECKED_CAST")
            val lineARow = lines["SKU-A"] as Map<String, Any?>
            @Suppress("UNCHECKED_CAST")
            val lineBRow = lines["SKU-B"] as Map<String, Any?>
            assertEquals(lineA, lineARow["amount"])
            assertEquals(lineB, lineBRow["amount"])
        }
    }

    @Test
    fun `AvroConverter with Schema Registry roundtrips all Avro types`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Avro's full type universe — primitives, compound (record/enum/array/map/union),
        // bytes, plus the logical types we map (decimal/date/time-millis/timestamp-millis).
        val schemaJson = """
            {
              "type": "record",
              "name": "AllTypes",
              "namespace": "xtdb.test",
              "fields": [
                {"name": "_id",          "type": "string"},
                {"name": "bool_val",     "type": "boolean"},
                {"name": "int_val",      "type": "int"},
                {"name": "long_val",     "type": "long"},
                {"name": "float_val",    "type": "float"},
                {"name": "double_val",   "type": "double"},
                {"name": "string_val",   "type": "string"},
                {"name": "bytes_val",    "type": "bytes"},
                {"name": "nullable_val", "type": ["null", "string"], "default": null},
                {"name": "enum_val", "type": {"type": "enum", "name": "Color",
                                              "symbols": ["RED", "GREEN", "BLUE"]}},
                {"name": "array_val",    "type": {"type": "array", "items": "int"}},
                {"name": "map_val",      "type": {"type": "map", "values": "string"}},
                {"name": "nested_val",   "type": {
                    "type": "record", "name": "Nested",
                    "fields": [{"name": "inner", "type": "string"}]
                }},
                {"name": "decimal_val",  "type": {"type": "bytes", "logicalType": "decimal",
                                                  "precision": 10, "scale": 2}},
                {"name": "date_val",     "type": {"type": "int",  "logicalType": "date"}},
                {"name": "time_val",     "type": {"type": "int",  "logicalType": "time-millis"}},
                {"name": "timestamp_val","type": {"type": "long", "logicalType": "timestamp-millis"}}
              ]
            }
        """.trimIndent()
        val avroSchema = AvroSchema.Parser().parse(schemaJson)

        val decimal = java.math.BigDecimal("123.45")
        val decimalBytes = org.apache.avro.Conversions.DecimalConversion().toBytes(
            decimal,
            avroSchema.getField("decimal_val").schema(),
            avroSchema.getField("decimal_val").schema().logicalType,
        )

        val expectedDate = java.time.LocalDate.of(2024, 6, 8)
        val expectedTime = java.time.LocalTime.of(12, 34, 56)
        val expectedTs = java.time.Instant.parse("2024-06-08T12:34:56.789Z")

        val daysSinceEpoch = expectedDate.toEpochDay().toInt()
        val millisOfDay = ((expectedTime.toNanoOfDay() / 1_000_000L).toInt())
        val tsMillis = expectedTs.toEpochMilli()

        val enumSchema = avroSchema.getField("enum_val").schema()
        val nestedSchema = avroSchema.getField("nested_val").schema()

        val rec = GenericData.Record(avroSchema).apply {
            put("_id", "all-types")
            put("bool_val", true)
            put("int_val", 42)
            put("long_val", 9999999999L)
            put("float_val", 3.14f)
            put("double_val", 2.71828)
            put("string_val", "hello")
            put("bytes_val", java.nio.ByteBuffer.wrap(byteArrayOf(0x01, 0x02, 0x03)))
            put("nullable_val", "maybe")
            put("enum_val", GenericData.EnumSymbol(enumSchema, "GREEN"))
            put("array_val", listOf(1, 2, 3))
            put("map_val", mapOf("k1" to "v1", "k2" to "v2"))
            put("nested_val", GenericData.Record(nestedSchema).apply { put("inner", "deep") })
            put("decimal_val", decimalBytes)
            put("date_val", daysSinceEpoch)
            put("time_val", millisOfDay)
            put("timestamp_val", tsMillis)
        }

        produceAvro(sourceTopic, "all-types", rec)

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attach(node, "events", """
                log: !Kafka
                  cluster: kafka
                  topic: test-replica-${UUID.randomUUID()}
                externalSource: !KafkaConnect
                  remote: kafka
                  topic: $sourceTopic
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: io.confluent.connect.avro.AvroConverter
                    value.converter.schema.registry.url: $schemaRegistryUrl
                  indexer: !Docs
                    table: events
            """.trimIndent())

            awaitCondition("record appears", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'all-types'").isNotEmpty()
            }

            val row = xtQueryDb(
                node, "events",
                "SELECT _id, bool_val, int_val, long_val, float_val, double_val, string_val, bytes_val, " +
                    "nullable_val, enum_val, array_val, map_val, nested_val, " +
                    "decimal_val, date_val, time_val, timestamp_val " +
                    "FROM public.events WHERE _id = 'all-types'",
            )[0]

            assertEquals(true, row["bool_val"])
            assertEquals(42L, (row["int_val"] as Number).toLong())
            assertEquals(9999999999L, (row["long_val"] as Number).toLong())
            assertEquals(3.14, (row["float_val"] as Number).toDouble(), 0.001)
            assertEquals(2.71828, (row["double_val"] as Number).toDouble(), 0.00001)
            assertEquals("hello", row["string_val"])

            val bytes = row["bytes_val"]
            assertNotNull(bytes)
            val byteArr: ByteArray = when (bytes) {
                is ByteArray -> bytes
                is java.nio.ByteBuffer -> ByteArray(bytes.remaining()).also { bytes.duplicate().get(it) }
                else -> error("unexpected bytes type: ${bytes!!::class}")
            }
            assertEquals(listOf<Byte>(0x01, 0x02, 0x03), byteArr.toList())

            assertEquals("maybe", row["nullable_val"])
            assertEquals("GREEN", row["enum_val"])

            assertEquals(listOf(1L, 2L, 3L), toList(row["array_val"]).map { (it as Number).toLong() })

            @Suppress("UNCHECKED_CAST")
            val map = row["map_val"] as Map<String, Any?>
            assertEquals(mapOf("k1" to "v1", "k2" to "v2"), map)

            @Suppress("UNCHECKED_CAST")
            val nested = row["nested_val"] as Map<String, Any?>
            assertEquals("deep", nested["inner"])

            // Logical types
            assertEquals(decimal, row["decimal_val"])
            assertEquals(expectedDate, toLocalDate(row["date_val"]))
            assertEquals(expectedTime, toLocalTime(row["time_val"]))
            assertEquals(expectedTs, toInstant(row["timestamp_val"]))
        }
    }
}
