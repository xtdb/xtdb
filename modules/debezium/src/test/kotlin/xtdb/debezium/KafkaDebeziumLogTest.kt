package xtdb.debezium

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.database.ExternalLog.MessageProcessor
import xtdb.debezium.proto.DebeziumOffsetToken
import xtdb.debezium.proto.debeziumOffsetToken
import xtdb.debezium.proto.partitionOffsets
import com.google.protobuf.Any as ProtoAny
import java.util.Collections
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaDebeziumLogTest {

    companion object {
        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            kafka.start()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            kafka.stop()
        }
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

    data class CapturedMessage(
        val txId: Long,
        val totalRows: Int,
        val offsets: DebeziumOffsetToken,
    )

    private fun capturingProcessor(): Pair<MessageProcessor<DebeziumMessage>, List<CapturedMessage>> {
        val received = Collections.synchronizedList(mutableListOf<CapturedMessage>())

        val capturing = MessageProcessor<DebeziumMessage> { msgs ->
            for (msg in msgs) {
                received.add(CapturedMessage(
                    txId = msg.txId,
                    totalRows = msg.openTx.tables.sumOf { (_, table) -> table.txRelation.rowCount },
                    offsets = msg.offsets,
                ))
            }
        }
        return (capturing to received)
    }

    @Test
    fun `subscription close cancels cleanly`() = runTest(timeout = 10.seconds) {
        val topic = "test-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val job = launch { log.tailAll(null, subscriber) }
            while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
            job.cancelAndJoin()
            produceMessages(topic, listOf(cdcMessage("c", 2, "Bob")))
            runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
        }

        assertEquals(1, received.size, "Should not receive messages after subscription close")
        assertEquals(1, received[0].totalRows)
    }

    @Test
    fun `log close cancels all subscriptions`() = runTest(timeout = 10.seconds) {
        val topic = "test-log-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)

        val job = launch { log.tailAll(null, subscriber) }
        while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }

        assertEquals(1, received.size, "Should have received Alice before closing")

        job.cancelAndJoin()
        log.close()

        produceMessages(topic, listOf(cdcMessage("c", 2, "Bob")))
        runInterruptible(Dispatchers.IO) { Thread.sleep(500) }

        assertEquals(1, received.size, "Should not receive messages after close")
    }

    @Test
    fun `tombstone offsets are tracked in cumulative token`() = runTest(timeout = 10.seconds) {
        val topic = "test-tombstone"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice"), null))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.tailAll(null, subscriber) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
                runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        val totalRows = received.sumOf { it.totalRows }
        assertEquals(1, totalRows)

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

        val token = ProtoAny.pack(debeziumOffsetToken {
            dbzmTopicOffsets[topic] = partitionOffsets { offsets += listOf(1L) }
        }, "xtdb.debezium")

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.tailAll(token, subscriber) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
                runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        val totalRows = received.sumOf { it.totalRows }
        assertEquals(1, totalRows)

        val lastToken = received.last().offsets
        val offsets = lastToken.dbzmTopicOffsetsMap[topic]!!.offsetsList
        assertEquals(2L, offsets[0], "Token should reflect latest consumed offset")
    }

    @Test
    fun `poll batch is collapsed into single message with multiple rows`() = runTest(timeout = 10.seconds) {
        val topic = "test-batch-collapse"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        val (subscriber, received) = capturingProcessor()
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.tailAll(null, subscriber) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
                runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        assertEquals(1, received.size, "Poll batch should be collapsed into a single message")
        assertEquals(3, received[0].totalRows)

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
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.tailAll(null, subscriber) }
            try {
                while (received.sumOf { it.totalRows } < 3) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        val lastToken = received.last().offsets
        val offsets = lastToken.dbzmTopicOffsetsMap[topic]!!.offsetsList
        assertEquals(2L, offsets[0], "Token should track the latest offset, not the first")
    }

    @Test
    fun `invalid JSON halts ingestion`() = runTest(timeout = 10.seconds) {
        val topic = "test-invalid-json"
        produceMessages(topic, listOf("not json at all"))

        val (subscriber, _) = capturingProcessor()
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            assertFailsWith<Exception> { runBlocking { log.tailAll(null, subscriber) } }
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
        val log = KafkaDebeziumLog("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            assertFailsWith<Exception> { runBlocking { log.tailAll(null, subscriber) } }
        }
    }
}
