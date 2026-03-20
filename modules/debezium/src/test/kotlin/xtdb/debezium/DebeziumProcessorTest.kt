package xtdb.debezium

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import org.apache.arrow.memory.RootAllocator
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.KafkaCluster
import xtdb.api.log.Log
import xtdb.api.log.Log.Record
import xtdb.api.log.SourceMessage
import xtdb.api.query.IKeyFn
import xtdb.arrow.Relation
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.*
import java.util.Collections.synchronizedList
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class DebeziumProcessorTest {

    companion object {
        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")

        @JvmStatic @BeforeAll
        fun startKafka() { kafka.start() }

        @JvmStatic @AfterAll
        fun stopKafka() { kafka.stop() }
    }

    private lateinit var allocator: RootAllocator

    @BeforeEach
    fun setUp() { allocator = RootAllocator() }

    @AfterEach
    fun tearDown() { allocator.close() }

    private fun putRecord(
        id: Int,
        name: String,
        op: String = "c",
        offset: Long = 0,
        table: String = "test",
    ): Record<DebeziumMessage> {
        val envelope = buildJsonObject {
            putJsonObject("payload") {
                put("op", op)
                putJsonObject("after") { put("_id", id); put("name", name) }
                put("before", JsonNull)
                putJsonObject("source") {
                    put("schema", "public")
                    put("table", table)
                    put("lsn", 100)
                }
            }
        }
        return Record(
            0,
            offset,
            Instant.now(),
            DebeziumMessage(
                envelope.toString().toByteArray(),
                emptyMap(),
                ConsumerGroupMetadata("test-group"),
            )
        )
    }

    private fun deleteRecord(
        id: Int,
        offset: Long = 0,
        table: String = "test",
    ): Record<DebeziumMessage> {
        val envelope = buildJsonObject {
            putJsonObject("payload") {
                put("op", "d")
                put("after", JsonNull)
                putJsonObject("before") { put("_id", id) }
                putJsonObject("source") {
                    put("schema", "public")
                    put("table", table)
                    put("lsn", 100)
                }
            }
        }
        return Record(
            0,
            offset,
            Instant.now(),
            DebeziumMessage(
                envelope.toString().toByteArray(),
                emptyMap(),
                ConsumerGroupMetadata("test-group"),
            )
        )
    }

    private fun rawRecord(payload: String, offset: Long = 0): Record<DebeziumMessage> =
        Record(
            0,
            offset,
            Instant.now(),
            DebeziumMessage(
                payload.toByteArray(),
                emptyMap(),
                ConsumerGroupMetadata("test-group"),
            )
        )

    private suspend fun <R> withDebeziumProducer(
        defaultTz: ZoneId = ZoneOffset.UTC,
        block: suspend (DebeziumProcessor, MutableList<Record<SourceMessage>>) -> R,
    ): R {
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val received = synchronizedList(mutableListOf<Record<SourceMessage>>())

        val cluster = KafkaCluster.ClusterFactory(kafka.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open()

        return cluster.use {
            KafkaCluster.LogFactory("kafka", sourceTopic, groupId = "xtdb-$sourceTopic-debezium")
                .openSourceLog(mapOf("kafka" to cluster)).use { log ->
                    log.tailAll(-1) { records -> received.addAll(records) }.use {
                        log.openAtomicProducer("test-debezium").use { producer ->
                            val processor = DebeziumProcessor(producer as KafkaCluster.AtomicProducer, allocator, defaultTz)
                            block(processor, received)
                        }
                    }
                }
        }
    }

    private fun decodeTx(msg: SourceMessage.Tx): Map<*, *> =
        Relation.openFromArrowStream(allocator, msg.payload).use { rel ->
            rel.toMaps(IKeyFn.KeyFn.SNAKE_CASE_STRING).first()
        }

    @Test
    fun `invalid JSON goes to DLQ`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            processor.processRecords(listOf(rawRecord("not json at all", offset = 42)))

            while (received.size < 1) delay(100)

            assertEquals(1, received.size)

            val tx = decodeTx(received[0].message as SourceMessage.Tx)
            val metadata = tx["user-metadata"] as Map<*, *>
            assertEquals("debezium", metadata["source"])
            assertTrue((metadata["error"] as String).contains("Invalid CDC message"))
            assertEquals(42L, metadata["kafka_offset"])
        }
    }

    @Test
    fun `empty record list is a no-op`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            processor.processRecords(emptyList())

            delay(1000)

            assertEquals(0, received.size)
        }
    }

    @Test
    fun `valid records in batch still processed when others fail`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            val batch = listOf(
                putRecord(1, "Alice", offset = 0),
                rawRecord("not json", offset = 1),
                putRecord(2, "Bob", offset = 2),
            )

            processor.processRecords(batch)

            while (received.size < 3) delay(100)

            assertEquals(3, received.size)

            // Alice — valid tx
            val tx0 = decodeTx(received[0].message as SourceMessage.Tx)
            assertTrue((tx0["tx-ops"] as List<*>).isNotEmpty())
            assertEquals(0L, (tx0["user-metadata"] as Map<*, *>)["kafka_offset"])

            // Invalid JSON — DLQ tx
            val tx1 = decodeTx(received[1].message as SourceMessage.Tx)
            assertTrue((tx1["tx-ops"] as List<*>).isEmpty())
            val dlqMetadata = tx1["user-metadata"] as Map<*, *>
            assertEquals("debezium", dlqMetadata["source"])
            assertEquals(1L, dlqMetadata["kafka_offset"])

            // Bob — valid tx
            val tx2 = decodeTx(received[2].message as SourceMessage.Tx)
            assertTrue((tx2["tx-ops"] as List<*>).isNotEmpty())
            assertEquals(2L, (tx2["user-metadata"] as Map<*, *>)["kafka_offset"])
        }
    }

    @Test
    fun `node failure propagates out of processRecords`() = runTest(timeout = 61.seconds) {
        val failingProducer = object : KafkaCluster.AtomicProducer<SourceMessage> {
            override fun openTx(): KafkaCluster.AtomicProducer.Tx<SourceMessage> =
                throw RuntimeException("Kafka unavailable")
            override fun close() {}
        }

        val processor = DebeziumProcessor(failingProducer, allocator, ZoneOffset.UTC)
        assertThrows<Exception> {
            processor.processRecords(listOf(rawRecord("doesn't matter")))
        }
    }

    @Test
    fun `batch of mixed ops processes all records`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            val batch = listOf(
                putRecord(1, "Alice", op = "c", offset = 0),
                putRecord(2, "Bob", op = "c", offset = 1),
                putRecord(1, "Alice Updated", op = "u", offset = 2),
                deleteRecord(2, offset = 3),
            )

            processor.processRecords(batch)

            while (received.size < 4) delay(100)

            assertEquals(4, received.size)

            received.forEachIndexed { idx, record ->
                val tx = decodeTx(record.message as SourceMessage.Tx)
                assertTrue((tx["tx-ops"] as List<*>).isNotEmpty())
                assertEquals(idx.toLong(), (tx["user-metadata"] as Map<*, *>)["kafka_offset"])
            }
        }
    }

    @Test
    fun `defaultTz is preserved in transaction`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer(defaultTz = ZoneId.of("America/Los_Angeles")) { processor, received ->
            processor.processRecords(listOf(putRecord(1, "Alice")))

            while (received.size < 1) delay(100)

            assertEquals(1, received.size)

            val tx = decodeTx(received[0].message as SourceMessage.Tx)
            assertEquals("America/Los_Angeles", tx["default-tz"])
        }
    }
}
