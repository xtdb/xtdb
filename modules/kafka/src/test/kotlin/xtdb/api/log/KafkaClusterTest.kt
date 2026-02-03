package xtdb.api.log

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.*
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Collections.synchronizedList
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaClusterTest {
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

    @Test
    fun `round-trips messages`() = runTest(timeout = 30.seconds) {
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        fun trieDetails(key: String, size: Long) =
            TrieDetails.newBuilder()
                .setTableName("my-table").setTrieKey(key)
                .setDataFileSize(size)
                .build()

        val addedTrieDetails = listOf(trieDetails("foo", 12), trieDetails("bar", 18))

        val databaseConfig = Database.Config(
            Log.localLog("log-path".asPath), Storage.local("storage-path".asPath)
        )

        val topicName = "test-topic-${UUID.randomUUID()}"

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.tailAll(subscriber, -1).use { _ ->
                            val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip().array()
                            log.appendMessage(Message.Tx(txPayload)).await()

                            log.appendMessage(Message.FlushBlock(12)).await()

                            log.appendMessage(Message.TriesAdded(Storage.VERSION, 0, addedTrieDetails)).await()

                            log.appendMessage(Message.AttachDatabase("foo", databaseConfig))

                            while (synchronized(msgs) { msgs.flatten().size } < 4) delay(100)
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }

        assertEquals(4, allMsgs.size)

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertEquals(42, ByteBuffer.wrap(it.payload).getLong(1))
        }

        allMsgs[1].message.let {
            check(it is Message.FlushBlock)
            assertEquals(12, it.expectedBlockIdx)
        }

        allMsgs[2].message.let {
            check(it is Message.TriesAdded)
            assertEquals(addedTrieDetails, it.tries)
        }

        allMsgs[3].message.let {
            check(it is Message.AttachDatabase)
            assertEquals("foo", it.dbName)
            assertEquals(databaseConfig, it.config)
        }
    }

    private fun txMessage(id: Byte) = Message.Tx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage returns null when topic is empty`() = runTest(timeout = 30.seconds)  {
        val topicName = "test-topic-${UUID.randomUUID()}"

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        assertEquals(null, log.readLastMessage())
                    }
            }
    }

    @Test
    fun `readLastMessage returns the message after appending one`() = runTest(timeout = 30.seconds)  {
        val topicName = "test-topic-${UUID.randomUUID()}"

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.appendMessage(txMessage(1)).await()

                        val lastMessage = log.readLastMessage()
                        check(lastMessage is Message.Tx)
                        assertArrayEquals(byteArrayOf(-1, 1), lastMessage.payload)
                    }
            }
    }

    @Test
    fun `readLastMessage returns the last message after appending multiple`() = runTest(timeout = 30.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.appendMessage(txMessage(1)).await()
                        log.appendMessage(txMessage(2)).await()
                        log.appendMessage(txMessage(3)).await()

                        val lastMessage = log.readLastMessage()
                        check(lastMessage is Message.Tx)
                        assertArrayEquals(byteArrayOf(-1, 3), lastMessage.payload)
                    }
            }
    }
}
