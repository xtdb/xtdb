package xtdb.debezium

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import xtdb.debezium.proto.DebeziumOffsetToken
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

    private fun putEvent(id: Int, name: String, table: String = "test") =
        CdcEvent.Put("public", table, mapOf("_id" to id, "name" to name))

    private fun deleteEvent(id: Int, table: String = "test") =
        CdcEvent.Delete("public", table, id)

    private fun testMessage(
        ops: List<CdcEvent>,
        offset: Long = 0,
    ) = DebeziumMessage(offset, Instant.now(), ops, DebeziumOffsetToken.getDefaultInstance(), ConsumerGroupMetadata("test-group"))

    private fun putMessage(id: Int, name: String, offset: Long = 0, table: String = "test") =
        testMessage(listOf(putEvent(id, name, table)), offset)

    private fun deleteMessage(id: Int, offset: Long = 0, table: String = "test") =
        testMessage(listOf(deleteEvent(id, table)), offset)

    private suspend fun <R> withDebeziumProducer(
        defaultTz: ZoneId = ZoneOffset.UTC,
        block: suspend (DebeziumProcessor, MutableList<Log.Record<SourceMessage>>) -> R,
    ): R {
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val received = synchronizedList(mutableListOf<Log.Record<SourceMessage>>())

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

    private fun decodeTx(msg: SourceMessage.Tx): Map<String, Any?> {
        val txOps = Relation.openFromArrowStream(allocator, msg.txOps).use { rel ->
            rel.toMaps(IKeyFn.KeyFn.SNAKE_CASE_STRING)
        }
        val userMetadata = msg.userMetadata?.let {
            xtdb.tx.deserializeUserMetadata(allocator, it)
        }
        return mapOf("tx-ops" to txOps, "user-metadata" to userMetadata)
    }

    @Test
    fun `empty record list is a no-op`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            processor.processMessages(emptyList())

            delay(1000)

            assertEquals(0, received.size)
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
            processor.processMessages(listOf(putMessage(1, "Alice")))
        }
    }

    @Test
    fun `batch of mixed ops processes all records`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            val batch = listOf(
                putMessage(1, "Alice", offset = 0),
                putMessage(2, "Bob", offset = 1),
                putMessage(1, "Alice Updated", offset = 2),
                deleteMessage(2, offset = 3),
            )

            processor.processMessages(batch)

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
            processor.processMessages(listOf(putMessage(1, "Alice")))

            while (received.size < 1) delay(100)

            assertEquals(1, received.size)

            val msg = received[0].message as SourceMessage.Tx
            assertEquals(ZoneId.of("America/Los_Angeles"), msg.defaultTz)
        }
    }

    @Test
    fun `empty ops list produces empty transaction`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            processor.processMessages(listOf(testMessage(emptyList())))

            while (received.size < 1) delay(100)

            assertEquals(1, received.size)
            val tx = decodeTx(received[0].message as SourceMessage.Tx)
            assertTrue((tx["tx-ops"] as List<*>).isEmpty())
        }
    }

    @Test
    fun `multi-op message produces single transaction`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            val record = testMessage(
                listOf(putEvent(1, "Alice"), putEvent(2, "Bob")),
                offset = 0
            )
            processor.processMessages(listOf(record))

            while (received.size < 1) delay(100)

            assertEquals(1, received.size)
            val tx = decodeTx(received[0].message as SourceMessage.Tx)
            assertEquals(2, (tx["tx-ops"] as List<*>).size)
        }
    }

    @Test
    fun `multi-op message with mixed put and delete`() = runTest(timeout = 60.seconds) {
        withDebeziumProducer { processor, received ->
            val record = testMessage(
                listOf(putEvent(1, "Alice"), deleteEvent(2)),
                offset = 0
            )
            processor.processMessages(listOf(record))

            while (received.size < 1) delay(100)

            assertEquals(1, received.size)
            val tx = decodeTx(received[0].message as SourceMessage.Tx)
            assertEquals(2, (tx["tx-ops"] as List<*>).size)
        }
    }

}
