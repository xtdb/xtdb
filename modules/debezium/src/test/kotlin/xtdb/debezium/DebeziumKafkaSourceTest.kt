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
import org.apache.arrow.memory.RootAllocator
import xtdb.api.TransactionKey
import xtdb.database.ExternalSource
import xtdb.database.ExternalSource.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.debezium.proto.DebeziumOffsetToken
import xtdb.debezium.proto.debeziumOffsetToken
import xtdb.debezium.proto.partitionOffsets
import xtdb.indexer.OpenTx
import com.google.protobuf.Any as ProtoAny
import java.time.Instant
import java.util.Collections
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class DebeziumKafkaSourceTest {

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

    data class CapturedTx(
        val cdcRows: Int,
        val resumeToken: ExternalSourceToken?,
        val result: TxResult,
    )

    class CapturingTxIndexer(
        val txIndexer: ExternalSource.TxIndexer,
        val received: List<CapturedTx>,
        private val allocator: RootAllocator,
    ) : AutoCloseable {
        override fun close() = allocator.close()
        operator fun component1() = txIndexer
        operator fun component2() = received
    }

    private fun capturingTxIndexer(): CapturingTxIndexer {
        val received = Collections.synchronizedList(mutableListOf<CapturedTx>())
        val al = RootAllocator()
        var nextTxId = 0L

        val txIndexer = object : ExternalSource.TxIndexer {
            override suspend fun indexTx(
                externalSourceToken: ExternalSourceToken?,
                systemTime: Instant?,
                writer: suspend (OpenTx) -> ExternalSource.TxResult,
            ): ExternalSource.TxResult {
                val txKey = TransactionKey(nextTxId++, systemTime ?: Instant.now())

                return OpenTx(al, txKey).use { openTx ->
                    val result = writer(openTx)

                    received.add(CapturedTx(
                        cdcRows = openTx.tables
                            .filter { (ref, _) -> ref.schemaName != "xt" }
                            .sumOf { (_, table) -> table.txRelation.rowCount },
                        resumeToken = externalSourceToken,
                        result = result,
                    ))

                    result
                }
            }
        }
        return CapturingTxIndexer(txIndexer, received, al)
    }

    private fun resumeTokenOffsets(token: ExternalSourceToken?, topic: String): Long? {
        if (token == null) return null
        val debeziumToken = token.unpack(DebeziumOffsetToken::class.java)
        return debeziumToken.dbzmTopicOffsetsMap[topic]?.offsetsList?.firstOrNull()
    }

    @Test
    fun `subscription close cancels cleanly`() = runTest(timeout = 10.seconds) {
        val topic = "test-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val job = launch { log.onPartitionAssigned(0, null, txIndexer) }
            while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
            job.cancelAndJoin()
            produceMessages(topic, listOf(cdcMessage("c", 2, "Bob")))
            runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
        }

        assertEquals(1, received.size, "Should not receive messages after subscription close")
        assertEquals(1, received[0].cdcRows)
    }

    @Test
    fun `log close cancels all subscriptions`() = runTest(timeout = 10.seconds) {
        val topic = "test-log-close"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)

        val job = launch { log.onPartitionAssigned(0, null, txIndexer) }
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

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.onPartitionAssigned(0, null, txIndexer) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
                runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        val cdcRows = received.sumOf { it.cdcRows }
        assertEquals(1, cdcRows)

        assertEquals(1L, resumeTokenOffsets(received.last().resumeToken, topic),
            "Token should track offset past the tombstone")
    }

    @Test
    fun `resumes from offset token`() = runTest(timeout = 10.seconds) {
        val topic = "test-resume"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        val afterToken = ProtoAny.pack(debeziumOffsetToken {
            dbzmTopicOffsets[topic] = partitionOffsets { offsets += listOf(1L) }
        }, "xtdb.debezium")

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.onPartitionAssigned(0, afterToken, txIndexer) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
                runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        val cdcRows = received.sumOf { it.cdcRows }
        assertEquals(1, cdcRows)

        assertEquals(2L, resumeTokenOffsets(received.last().resumeToken, topic),
            "Token should reflect latest consumed offset")
    }

    @Test
    fun `poll batch is collapsed into single tx with multiple rows`() = runTest(timeout = 10.seconds) {
        val topic = "test-batch-collapse"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.onPartitionAssigned(0, null, txIndexer) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
                runInterruptible(Dispatchers.IO) { Thread.sleep(500) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        assertEquals(1, received.size, "Poll batch should be collapsed into a single tx")
        assertEquals(3, received[0].cdcRows)

        assertEquals(2L, resumeTokenOffsets(received[0].resumeToken, topic))
    }

    @Test
    fun `offset token tracks latest offset per topic`() = runTest(timeout = 10.seconds) {
        val topic = "test-latest-offset"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            cdcMessage("c", 2, "Bob"),
            cdcMessage("c", 3, "Carol"),
        ))

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val tailJob = launch { log.onPartitionAssigned(0, null, txIndexer) }
            try {
                while (received.sumOf { it.cdcRows } < 3) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
            } finally {
                tailJob.cancelAndJoin()
            }
        }

        assertEquals(2L, resumeTokenOffsets(received.last().resumeToken, topic),
            "Token should track the latest offset, not the first")
    }

    @Test
    fun `invalid JSON halts ingestion`() = runTest(timeout = 10.seconds) {
        val topic = "test-invalid-json"
        produceMessages(topic, listOf("not json at all"))

        val (txIndexer, _) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            assertFailsWith<Exception> { runBlocking { log.onPartitionAssigned(0, null, txIndexer) } }
        }
    }

    @Test
    fun `invalid record after valid records halts ingestion`() = runTest(timeout = 10.seconds) {
        val topic = "test-valid-then-invalid"
        produceMessages(topic, listOf(
            cdcMessage("c", 1, "Alice"),
            "not json",
        ))

        val (txIndexer, _) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            assertFailsWith<Exception> { runBlocking { log.onPartitionAssigned(0, null, txIndexer) } }
        }
    }

    @Test
    fun `userMetadata carries lsn and tx_us`() = runTest(timeout = 10.seconds) {
        val topic = "test-user-metadata"
        produceMessages(topic, listOf(cdcMessage("c", 1, "Alice")))

        val (txIndexer, received) = capturingTxIndexer()
        val log = DebeziumKafkaSource("testdb", kafkaConfig(), topic, MessageFormat.Json)
        log.use {
            val job = launch { log.onPartitionAssigned(0, null, txIndexer) }
            try {
                while (received.isEmpty()) runInterruptible(Dispatchers.IO) { Thread.sleep(100) }
            } finally {
                job.cancelAndJoin()
            }
        }

        val result = received[0].result as TxResult.Committed
        val metadata = result.userMetadata!!
        assertTrue(metadata.containsKey("lsn"), "userMetadata should contain lsn")
        assertTrue(metadata.containsKey("tx_us"), "userMetadata should contain tx_us")
        assertEquals(0L, metadata["lsn"], "lsn should be the max Kafka offset")
        assertTrue((metadata["tx_us"] as Long) > 0, "tx_us should be a positive micros timestamp")
    }
}
