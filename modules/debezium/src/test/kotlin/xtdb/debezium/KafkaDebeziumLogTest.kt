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
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log
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

    private fun produceMessages(topic: String, messages: List<String>) {
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

    private fun capturingProcessor(): Pair<Log.RecordProcessor<DebeziumMessage>, List<Log.Record<DebeziumMessage>>> {
        val received = Collections.synchronizedList(mutableListOf<Log.Record<DebeziumMessage>>())

        val capturing = Log.RecordProcessor { records ->
            received.addAll(records)
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
            log.tailAll(subscriber).use {
                while (received.isEmpty()) delay(100)
            }
            // Subscription closed — produce more messages
            produceMessages(topic, listOf(cdcMessage("c", 2, "Bob")))
            delay(500)
        }

        // Should only have the first message — subscription was closed before Bob
        assertEquals(1, received.size, "Should not receive messages after subscription close")
    }

    @Test
    fun `log close cancels all subscriptions`() = runTest(timeout = 10.seconds) {
        val topic = "test-log-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog(kafkaConfig(), topic, "test-group")

        val subscription = log.tailAll(subscriber)
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
}
