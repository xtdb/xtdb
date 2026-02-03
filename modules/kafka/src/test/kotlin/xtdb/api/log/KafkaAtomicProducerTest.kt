package xtdb.api.log

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.*
import java.time.Duration
import java.util.Collections.synchronizedList
import java.util.UUID
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

    private fun txMessage(id: Byte) = Message.Tx(byteArrayOf(-1, id))

    @Test
    fun `committed transaction messages are visible to subscribers`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, -1).use {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(1))
                                    tx.appendMessage(txMessage(2))
                                    tx.commit()
                                }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 2) delay(100)
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(2, allMsgs.size)

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertArrayEquals(byteArrayOf(-1, 1), it.payload)
        }
        allMsgs[1].message.let {
            check(it is Message.Tx)
            assertArrayEquals(byteArrayOf(-1, 2), it.payload)
        }
    }

    @Test
    fun `aborted transaction messages are not visible to subscribers`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, -1).use {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                // Abort this transaction
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(1))
                                    tx.appendMessage(txMessage(2))
                                    tx.abort()
                                }

                                // Commit this one so subscriber gets something
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(3))
                                    tx.commit()
                                }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 1) delay(100)

                            // Give extra time to ensure no more messages arrive
                            delay(500)
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(1, allMsgs.size)

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertArrayEquals(byteArrayOf(-1, 3), it.payload)
        }
    }

    @Test
    fun `multiple sequential transactions on same producer`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, -1).use {
                            log.openAtomicProducer("tx-producer-1").use { producer ->
                                // First transaction
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(1))
                                    tx.commit()
                                }

                                // Second transaction
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(2))
                                    tx.appendMessage(txMessage(3))
                                    tx.commit()
                                }

                                // Third transaction
                                producer.openTx().use { tx ->
                                    tx.appendMessage(txMessage(4))
                                    tx.commit()
                                }
                            }

                            while (synchronized(msgs) { msgs.flatten().size } < 4) delay(100)
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(4, allMsgs.size)

        val payloads = allMsgs.map { (it.message as Message.Tx).payload[1] }
        assertEquals(listOf<Byte>(1, 2, 3, 4), payloads)
    }

    @Test
    fun `uncommitted transaction messages not visible until commit`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, -1).use {
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
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }
        assertEquals(1, allMsgs.size)
    }
}
