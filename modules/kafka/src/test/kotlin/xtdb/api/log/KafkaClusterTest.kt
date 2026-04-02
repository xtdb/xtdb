package xtdb.api.log

import com.google.protobuf.ByteString
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.RecordTooLargeException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.*
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
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
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())

        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
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
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        val job = launch { log.tailAll(-1, subscriber) }
                        try {
                            val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip().array()
                            log.appendMessage(SourceMessage.LegacyTx(txPayload))

                            log.appendMessage(SourceMessage.FlushBlock(12))

                            log.appendMessage(SourceMessage.TriesAdded(Storage.VERSION, 0, addedTrieDetails))

                            log.appendMessage(SourceMessage.AttachDatabase("foo", databaseConfig))

                            while (synchronized(msgs) { msgs.flatten().size } < 4) delay(100)
                        } finally {
                            job.cancelAndJoin()
                        }
                    }
            }

        val allMsgs = synchronized(msgs) { msgs.flatten() }

        assertEquals(4, allMsgs.size)

        allMsgs[0].message.let {
            check(it is SourceMessage.LegacyTx)
            assertEquals(42, ByteBuffer.wrap(it.payload).getLong(1))
        }

        allMsgs[1].message.let {
            check(it is SourceMessage.FlushBlock)
            assertEquals(12, it.expectedBlockIdx)
        }

        allMsgs[2].message.let {
            check(it is SourceMessage.TriesAdded)
            assertEquals(addedTrieDetails, it.tries)
        }

        allMsgs[3].message.let {
            check(it is SourceMessage.AttachDatabase)
            assertEquals("foo", it.dbName)
            assertEquals(databaseConfig, it.config)
        }
    }

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage returns null when topic is empty`() = runTest(timeout = 30.seconds)  {
        val topicName = "test-topic-${UUID.randomUUID()}"

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openSourceLog(mapOf("my-cluster" to cluster))
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
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.appendMessage(txMessage(1))

                        val lastMessage = log.readLastMessage()
                        check(lastMessage is SourceMessage.LegacyTx)
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
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.appendMessage(txMessage(1))
                        log.appendMessage(txMessage(2))
                        log.appendMessage(txMessage(3))

                        val lastMessage = log.readLastMessage()
                        check(lastMessage is SourceMessage.LegacyTx)
                        assertArrayEquals(byteArrayOf(-1, 3), lastMessage.payload)
                    }
            }
    }

    /**
     * Builds a BlockUploaded replica message >1MB by packing 256 TrieDetails with 8KB iid_bloom each.
     */
    private fun largeBlockUploaded(): ReplicaMessage.BlockUploaded {
        val bloomBytes = ByteString.copyFrom(ByteArray(8 * 1024))
        val tries = (0 until 256).map { i ->
            TrieDetails.newBuilder()
                .setTableName("table-$i")
                .setTrieKey("trie-key-$i")
                .setDataFileSize(1024L * (i + 1))
                .setTrieMetadata(trieMetadata { iidBloom = bloomBytes })
                .build()
        }

        return ReplicaMessage.BlockUploaded(
            storageVersion = Storage.VERSION,
            storageEpoch = 0,
            blockIndex = 42,
            latestProcessedMsgId = 100,
            tries = tries
        ).also {
            val encodedSize = it.encode().size
            assert(encodedSize > 1024 * 1024) { "Expected >1MB, got $encodedSize bytes" }
        }
    }

    @Test
    fun `round-trips large replica BlockUploaded message with increased message size`() = runTest(timeout = 60.seconds) {
        val eightMB = 8 * 1024 * 1024
        val fourMB = 4 * 1024 * 1024
        val topicName = "test-topic-${UUID.randomUUID()}"
        val replicaTopicName = "$topicName-replica"

        val largeMessageContainer = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withEnv("KAFKA_MESSAGE_MAX_BYTES", eightMB.toString())
            .withEnv("KAFKA_REPLICA_FETCH_MAX_BYTES", eightMB.toString())

        largeMessageContainer.start()
        try {
            AdminClient.create(mapOf("bootstrap.servers" to largeMessageContainer.bootstrapServers)).use { admin ->
                admin.createTopics(
                    listOf(
                        NewTopic(replicaTopicName, 1, 1)
                            .configs(
                                mapOf(
                                    "message.timestamp.type" to "LogAppendTime",
                                    "max.message.bytes" to eightMB.toString()
                                )
                            )
                    )
                ).all().get()
            }

            val blockUploaded = largeBlockUploaded()

            val msgs = synchronizedList(mutableListOf<List<Record<ReplicaMessage>>>())

            val subscriber = mockk<RecordProcessor<ReplicaMessage>> {
                coEvery { processRecords(capture(msgs)) } returns Unit
            }

            KafkaCluster.ClusterFactory(largeMessageContainer.bootstrapServers)
                .propertiesMap(
                    mapOf(
                        "max.request.size" to fourMB.toString(),
                        "fetch.max.bytes" to fourMB.toString(),
                        "max.partition.fetch.bytes" to fourMB.toString()
                    )
                )
                .pollDuration(Duration.ofMillis(100))
                .open().use { cluster ->
                    KafkaCluster.LogFactory("my-cluster", topicName, autoCreateTopic = false)
                        .openReplicaLog(mapOf("my-cluster" to cluster))
                        .use { log ->
                            val job = launch { log.tailAll(-1, subscriber) }
                            try {
                                log.appendMessage(blockUploaded)

                                while (synchronized(msgs) { msgs.flatten().size } < 1) delay(100)
                            } finally {
                                job.cancelAndJoin()
                            }
                        }
                }

            val allMsgs = synchronized(msgs) { msgs.flatten() }
            assertEquals(1, allMsgs.size)

            allMsgs[0].message.let {
                check(it is ReplicaMessage.BlockUploaded)
                assertEquals(42, it.blockIndex)
                assertEquals(100, it.latestProcessedMsgId)
                assertEquals(Storage.VERSION, it.storageVersion)
                assertEquals(256, it.tries.size)
                assertEquals("table-0", it.tries[0].tableName)
                assertEquals("trie-key-255", it.tries[255].trieKey)
            }
        } finally {
            largeMessageContainer.stop()
        }
    }

    @Test
    fun `large message fails with default producer config`() = runTest(timeout = 60.seconds) {
        val topicName = "test-topic-${UUID.randomUUID()}"
        val blockUploaded = largeBlockUploaded()

        // Default producer max.request.size is 1MB — sending a >1MB message should fail
        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topicName)
                    .openReplicaLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        assertThrows<RecordTooLargeException> {
                            log.appendMessage(blockUploaded)
                        }
                    }
            }
    }
}
