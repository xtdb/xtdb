package xtdb.debezium

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.database.ExternalLog.MessageProcessor
import xtdb.debezium.proto.debeziumOffsetToken
import xtdb.debezium.proto.partitionOffsets
import com.google.protobuf.Any as ProtoAny
import java.util.Collections
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaDebeziumLogTest {

    private lateinit var kafka: ConfluentKafkaContainer

    @BeforeEach
    fun setUp() {
        kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
        kafka.start()
    }

    @AfterEach
    fun tearDown() {
        kafka.stop()
    }

    private fun cdcMessage(op: String, id: Int, name: String): String {
        return buildJsonObject {
            putJsonObject("payload") {
                put("op", op)
                putJsonObject("after") { put("_id", id); put("name", name) }
                put("before", JsonNull)
                putJsonObject("source") {
                    put("schema", "public")
                    put("table", "test")
                    put("lsn", 100)
                }
            }
        }.toString()
    }

    private fun kafkaConfig() = mapOf("bootstrap.servers" to kafka.bootstrapServers)

    private fun produceMessages(topic: String, messages: List<String?>) {
        KafkaProducer<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            )
        ).use { producer ->
            for (msg in messages) {
                producer.send(ProducerRecord(topic, null, msg)).get()
            }
        }
    }

    private fun capturingProcessor(): Pair<MessageProcessor<DebeziumMessage>, List<DebeziumMessage>> {
        val received = Collections.synchronizedList(mutableListOf<DebeziumMessage>())

        val capturing = MessageProcessor<DebeziumMessage> { msgs ->
            received.addAll(msgs)
        }
        return (capturing to received)
    }

    @Test
    fun `subscription close cancels cleanly`() = runTest(timeout = 10.seconds) {
        val topic = "test-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(null, subscriber).use {
                while (received.isEmpty()) delay(100)
            }
            // Subscription closed — produce more messages
            produceMessages(topic, listOf(cdcMessage("c", 2, "Bob")))
            delay(500)
        }

        // Should only have the first message — subscription was closed before Bob
        assertEquals(1, received.size, "Should not receive messages after subscription close")

        // Verify the Log parsed the raw bytes into a CdcEvent
        val event = received[0].ops[0] as CdcEvent.Put
        assertEquals("public", event.schema)
        assertEquals("test", event.table)
        assertEquals(1L, event.doc["_id"])
        assertEquals("Alice", event.doc["name"])
    }

    @Test
    fun `log close cancels all subscriptions`() = runTest(timeout = 10.seconds) {
        val topic = "test-log-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")

        val subscription = log.tailAll(null, subscriber)
        while (received.isEmpty()) delay(100)

        assertEquals(1, received.size, "Should have received Alice before closing")

        // Close log directly (not via subscription) — should cancel the poll coroutine
        log.close()

        produceMessages(topic, listOf(cdcMessage("c", 2, "Bob")))
        delay(500)

        assertEquals(1, received.size, "Should not receive messages after log close")

        // Closing subscription after log close should not throw
        subscription.close()
    }

    @Test
    fun `tombstone offsets are tracked in cumulative token`() = runTest(timeout = 10.seconds) {
        val topic = "test-tombstone"
        // Real message at offset 0, tombstone at offset 1
        // The tombstone is last — if its offset isn't tracked, the token will show 0 instead of 1
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice"), null))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(null, subscriber).use {
                while (received.isEmpty()) delay(100)
                delay(500)
            }
        }

        // Only Alice should be delivered (tombstone skipped)
        val allOps = received.flatMap { it.ops }
        assertEquals(1, allOps.size)

        // Token should reflect offset 1 (the tombstone), not just offset 0 (Alice)
        val token = received.last().offsets
        val offsets = token.dbzmTopicOffsetsMap[topic]!!.offsetsList
        assertEquals(1L, offsets[0], "Token should track offset past the tombstone")
    }

    @Test
    fun `resumes from offset token`() = runTest(timeout = 10.seconds) {
        val topic = "test-resume"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        // Token saying we've already seen offset 1 on partition 0
        val token = ProtoAny.pack(debeziumOffsetToken {
            dbzmTopicOffsets[topic] = partitionOffsets { offsets += listOf(1L) }
        }, "xtdb.debezium")

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(token, subscriber).use {
                while (received.isEmpty()) delay(100)
                delay(500)
            }
        }

        // Should only get Carol (offset 2)
        val allOps = received.flatMap { it.ops }
        assertEquals(1, allOps.size)
        assertEquals(3L, (allOps[0] as CdcEvent.Put).doc["_id"])

        val lastToken = received.last().offsets
        val offsets = lastToken.dbzmTopicOffsetsMap[topic]!!.offsetsList
        assertEquals(2L, offsets[0], "Token should reflect latest consumed offset")
    }

    @Test
    fun `poll batch is collapsed into single message with multiple ops`() = runTest(timeout = 10.seconds) {
        val topic = "test-batch-collapse"
        // Produce 3 messages before consumer starts — they'll be in the same poll batch
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(null, subscriber).use {
                while (received.isEmpty()) delay(100)
                delay(500) // give time — should not receive additional messages
            }
        }

        // All pre-produced records should land in a single poll → single message
        assertEquals(1, received.size, "Poll batch should be collapsed into a single message")
        assertEquals(3, received[0].ops.size)

        val ids = received[0].ops.map { (it as CdcEvent.Put).doc["_id"] }.toSet()
        assertEquals(setOf(1L, 2L, 3L), ids)

        // Token should reflect the latest offset (2, since offsets are 0-indexed)
        val lastToken = received[0].offsets
        val offsets = lastToken.dbzmTopicOffsetsMap[topic]!!.offsetsList
        assertEquals(2L, offsets[0])
    }

    @Test
    fun `offset token tracks latest offset per topic`() = runTest(timeout = 10.seconds) {
        val topic = "test-latest-offset"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(null, subscriber).use {
                while (received.sumOf { it.ops.size } < 3) delay(100)
            }
        }

        // Last message's token should have offset 2 (the latest), not 0
        val lastToken = received.last().offsets
        val offsets = lastToken.dbzmTopicOffsetsMap[topic]!!.offsetsList
        assertEquals(2L, offsets[0], "Token should track the latest offset, not the first")
    }

    @Test
    fun `invalid JSON halts ingestion`() = runTest(timeout = 10.seconds) {
        val topic = "test-invalid-json"
        produceMessages(topic, listOf("not json at all"))

        val (subscriber, _) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(null, subscriber)
            val error = log.awaitError()
            assertTrue(error is Exception, "Expected an exception from invalid JSON")
        }
    }

    @Test
    fun `invalid record after valid records halts ingestion`() = runTest(timeout = 10.seconds) {
        val topic = "test-valid-then-invalid"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            "not json",
        ))

        val (subscriber, _) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")
        log.use {
            log.tailAll(null, subscriber)
            val error = log.awaitError()
            assertTrue(error is Exception)
        }
    }
}
