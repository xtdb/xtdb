package xtdb.api.log

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.*
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaSubscribeTest {
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
    fun `assignment callback fires on subscribe`() = runTest(timeout = 30.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group-${UUID.randomUUID()}"

        val assignedPartitions = AtomicReference<Collection<Int>>(null)

        val subscriber = mockk<Subscriber> {
            every { processRecords(any()) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        val listener = object : AssignmentListener {
            override fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset> {
                assignedPartitions.set(partitions)
                return emptyMap()
            }
            override fun onPartitionsRevoked(partitions: Collection<Int>) {}
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .groupId(groupId)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, listener).use {
                            while (assignedPartitions.get() == null) delay(50)
                        }
                    }
            }

        assertEquals(listOf(0), assignedPartitions.get()?.toList())
    }

    @Test
    fun `returned offsets are used for seeking`() = runTest(timeout = 30.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group-${UUID.randomUUID()}"

        val receivedRecords = Collections.synchronizedList(mutableListOf<Record>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(any()) } answers {
                receivedRecords.addAll(firstArg())
            }
            every { latestProcessedMsgId } returns -1
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .groupId(groupId)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        // Append some messages first
                        log.appendMessage(txMessage(0)).await()
                        log.appendMessage(txMessage(1)).await()
                        log.appendMessage(txMessage(2)).await()

                        // Subscribe starting at offset 2 (skip first two messages)
                        val listener = object : AssignmentListener {
                            override fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset> {
                                return mapOf(0 to 2L)
                            }
                            override fun onPartitionsRevoked(partitions: Collection<Int>) {}
                        }

                        log.subscribe(subscriber, listener).use {
                            while (synchronized(receivedRecords) { receivedRecords.size } < 1) delay(50)
                        }
                    }
            }

        synchronized(receivedRecords) {
            assertTrue(receivedRecords.isNotEmpty())
            val firstRecord = receivedRecords.first()
            assertEquals(2L, firstRecord.logOffset)
        }
    }

    @Test
    fun `revocation callback fires on close`() = runTest(timeout = 30.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group-${UUID.randomUUID()}"

        val revokedPartitions = AtomicReference<Collection<Int>>(null)
        val assigned = AtomicBoolean(false)

        val subscriber = mockk<Subscriber> {
            every { processRecords(any()) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        val listener = object : AssignmentListener {
            override fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset> {
                assigned.set(true)
                return emptyMap()
            }
            override fun onPartitionsRevoked(partitions: Collection<Int>) {
                revokedPartitions.set(partitions)
            }
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .groupId(groupId)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, listener).use {
                            while (!assigned.get()) delay(50)
                        }
                    }
            }

        assertEquals(listOf(0), revokedPartitions.get()?.toList())
    }

    @Test
    fun `subscribe without groupId throws`() {
        val topicName = "test-topic-${UUID.randomUUID()}"

        val subscriber = mockk<Subscriber>()
        val listener = mockk<AssignmentListener>()

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    // no groupId set
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        val ex = assertThrows(IllegalArgumentException::class.java) {
                            log.subscribe(subscriber, listener)
                        }
                        assertTrue(ex.message?.contains("groupId") == true)
                    }
            }
    }
}
