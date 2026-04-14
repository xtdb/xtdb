package xtdb.api.log

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.Log.Record
import xtdb.api.log.Log.RecordProcessor
import java.time.Duration
import java.util.*
import java.util.Collections.synchronizedList
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaAtomicProducerTest {
    companion object {
        private val container = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            container.start()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            container.stop()
        }
    }

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `committed transaction messages are visible to subscribers`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())

        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openSourceLog(mapOf("my-cluster" to cluster)).use { log ->
                        val tailJob = launch { log.tailAll(-1, subscriber) }
                        try {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                producer.withTx { tx ->
                                    tx.appendMessage(txMessage(1))
                                    tx.appendMessage(txMessage(2))
                                }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 2) delay(100)
                        } finally {
                            tailJob.cancelAndJoin()
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(2, allMsgs.size)

        allMsgs[0].message.let {
            check(it is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 1), it.payload)
        }
        allMsgs[1].message.let {
            check(it is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 2), it.payload)
        }
    }

    @Test
    fun `aborted transaction messages are not visible to subscribers`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())

        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        val tailJob = launch { log.tailAll(-1, subscriber) }
                        try {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                // Abort this transaction
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(1))
                                    tx.appendMessage(txMessage(2))
                                    tx.abort()
                                }

                                // Commit this one so subscriber gets something
                                producer.withTx { tx ->
                                    tx.appendMessage(txMessage(3))
                                }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 1) delay(100)

                            // Give extra time to ensure no more messages arrive
                            delay(500)
                        } finally {
                            tailJob.cancelAndJoin()
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(1, allMsgs.size)

        allMsgs[0].message.let {
            check(it is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 3), it.payload)
        }
    }

    @Test
    fun `multiple sequential transactions on same producer`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())

        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        val tailJob = launch { log.tailAll(-1, subscriber) }
                        try {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                producer.withTx { tx -> tx.appendMessage(txMessage(1)) }
                                producer.withTx { tx ->
                                    tx.appendMessage(txMessage(2))
                                    tx.appendMessage(txMessage(3))
                                }
                                producer.withTx { tx -> tx.appendMessage(txMessage(4)) }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 4) delay(100)
                        } finally {
                            tailJob.cancelAndJoin()
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(4, allMsgs.size)

        val payloads = allMsgs.map { (it.message as SourceMessage.LegacyTx).payload[1] }
        assertEquals(listOf<Byte>(1, 2, 3, 4), payloads)
    }

    @Test
    fun `uncommitted transaction messages not visible until commit`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())

        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        val tailJob = launch { log.tailAll(-1, subscriber) }
                        try {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(1))

                                    // Wait and verify nothing received yet
                                    delay(500)
                                    assertEquals(0, synchronized(msgs) { msgs.flatten().size })

                                    tx.commit()
                                }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 1) delay(100)
                        } finally {
                            tailJob.cancelAndJoin()
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(1, allMsgs.size)
    }

    @Test
    fun `sendOffsetsToTransaction commits consumer offsets atomically`() = runTest(timeout = 60.seconds) {
        // [input topic] → [consumer] → [processor] → [output topic]
        val outputTopic = "test-output-${UUID.randomUUID()}"
        val inputTopic = "test-input-${UUID.randomUUID()}"
        val consumerGroupId = "test-group-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())

        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }

        KafkaProducer(
            mapOf("bootstrap.servers" to container.bootstrapServers),
            StringSerializer(),
            StringSerializer()
        ).use { inputProducer ->
            inputProducer.send(ProducerRecord(inputTopic, "key", "value")).get()
        }

        val inputConsumerProps = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        )

        KafkaConsumer(inputConsumerProps, StringDeserializer(), StringDeserializer()).use { inputConsumer ->
            inputConsumer.subscribe(listOf(inputTopic))
            var records = inputConsumer.poll(Duration.ofSeconds(5))
            while (records.isEmpty) {
                records = inputConsumer.poll(Duration.ofSeconds(5))
            }
            val record = records.first()
            val offsets = mapOf(TopicPartition(record.topic(), record.partition()) to OffsetAndMetadata(record.offset() + 1))
            val groupMetadata = inputConsumer.groupMetadata()

            KafkaCluster.ClusterFactory(container.bootstrapServers)
                .pollDuration(Duration.ofMillis(100))
                .open().use { cluster ->
                    KafkaCluster.LogFactory("my-cluster", outputTopic)
                        .openSourceLog(mapOf("my-cluster" to cluster)).use { log ->
                            val tailJob = launch { log.tailAll(-1, subscriber) }
                            try {
                                (log.openAtomicProducer("tx-producer-1") as KafkaCluster.AtomicProducer).use { producer ->
                                    producer.openTx().use { tx ->
                                        tx.appendMessage(txMessage(1))
                                        tx.sendOffsetsToTransaction(offsets, groupMetadata)
                                        tx.commit()
                                    }
                                }

                                while (synchronized(msgs) { msgs.flatten().size } < 1) delay(100)
                            } finally {
                                tailJob.cancelAndJoin()
                            }
                        }
                }
        }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(1, allMsgs.size)
        allMsgs[0].message.let {
            check(it is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 1), it.payload)
        }

        // New consumer in same group gets nothing — offsets were committed
        KafkaConsumer(inputConsumerProps, StringDeserializer(), StringDeserializer()).use { c2 ->
            c2.subscribe(listOf(inputTopic))
            // Ensure we're assigned the topic before polling
            while (c2.assignment().isEmpty()) c2.poll(Duration.ofMillis(100))
            val records = c2.poll(Duration.ofSeconds(2))
            assertEquals(0, records.count(), "No records should be re-delivered — offsets were committed")
        }
    }

    @Test
    fun `second producer with same transactional id fences the first`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openSourceLog(mapOf("my-cluster" to cluster)).use { log ->
                        val producer1 = log.openAtomicProducer("same-tx-id")

                        // Commit succeeds before fencing
                        producer1.withTx { tx -> tx.appendMessage(txMessage(1)) }

                        // Second producer with the same transactional.id fences producer1
                        val producer2 = log.openAtomicProducer("same-tx-id")

                        // producer1 is now fenced — next commit should throw
                        val e = assertThrows(Exception::class.java) {
                            producer1.withTx { tx -> tx.appendMessage(txMessage(2)) }
                        }
                        assertTrue(producer1.isProducerFenced(e), "Should be recognized as a fencing exception")

                        producer1.close()
                        producer2.close()
                    }
            }
    }
}
