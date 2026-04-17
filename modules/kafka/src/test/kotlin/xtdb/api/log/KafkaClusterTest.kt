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
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.*
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.util.asPath
import xtdb.util.closeAll
import java.nio.ByteBuffer
import java.time.Duration
import java.util.*
import java.util.Collections.synchronizedList
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
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

                            while (synchronized(msgs) { msgs.flatten().size } < 4) delay(100.milliseconds)
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
    fun `readLastMessage returns null when topic is empty`() = runTest(timeout = 30.seconds) {
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
    fun `readLastMessage returns the message after appending one`() = runTest(timeout = 30.seconds) {
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
    fun `round-trips large replica BlockUploaded message with increased message size`() =
        runTest(timeout = 60.seconds) {
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

                                    while (synchronized(msgs) { msgs.flatten().size } < 1) delay(100.milliseconds)
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

    // SharedGroupConsumer integration tests
    private suspend fun withClusterAndLogs(
        topicNames: List<String>,
        block: suspend (KafkaCluster, List<Log<SourceMessage>>) -> Unit,
    ) {
        val cluster =
            KafkaCluster.ClusterFactory(container.bootstrapServers)
                .pollDuration(Duration.ofMillis(100))
                .open()

        cluster.use {
            val logs = topicNames.map { topic ->
                KafkaCluster.LogFactory("c", topic).openSourceLog(mapOf("c" to cluster))
            }
            try {
                block(cluster, logs)
            } finally {
                logs.closeAll()
            }
        }
    }

    private class TrackingListener(
        private val afterMsgId: MessageId = -1L,
    ) : SubscriptionListener<SourceMessage> {
        val assignedPartitions = CopyOnWriteArrayList<Collection<Int>>()
        val revokedPartitions = CopyOnWriteArrayList<Collection<Int>>()
        val records = CopyOnWriteArrayList<Record<SourceMessage>>()

        val isAssigned get() = assignedPartitions.size > revokedPartitions.size

        private val processor = RecordProcessor<SourceMessage> { recs -> records.addAll(recs) }

        override suspend fun onPartitionsAssigned(partitions: Collection<Int>): TailSpec<SourceMessage> {
            assignedPartitions.add(partitions)
            return TailSpec(afterMsgId, processor)
        }

        override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {
            revokedPartitions.add(partitions)
        }
    }

    @Test
    fun `shared group consumer delivers records to multiple databases`() = runTest(timeout = 60.seconds) {
        val topic1 = "test-shared-multi-${UUID.randomUUID()}"
        val topic2 = "test-shared-multi-${UUID.randomUUID()}"

        withClusterAndLogs(listOf(topic1, topic2)) { _, logs ->
            val (log1, log2) = logs
            val listener1 = TrackingListener()
            val listener2 = TrackingListener()

            val job1 = launch { log1.openGroupSubscription(listener1) }
            val job2 = launch { log2.openGroupSubscription(listener2) }

            while (!listener1.isAssigned || !listener2.isAssigned) delay(100.milliseconds)

            log1.appendMessage(txMessage(1))
            log2.appendMessage(txMessage(2))

            while (listener1.records.isEmpty() || listener2.records.isEmpty()) delay(100.milliseconds)

            assertEquals(1, listener1.records.size)
            assertEquals(1, listener2.records.size)

            val msg1 = listener1.records[0].message
            check(msg1 is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 1), msg1.payload)

            val msg2 = listener2.records[0].message
            check(msg2 is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 2), msg2.payload)

            job1.cancelAndJoin()
            job2.cancelAndJoin()
        }
    }

    @Test
    fun `unsubscribing one database does not affect others`() = runTest(timeout = 60.seconds) {
        val topic1 = "test-shared-unsub-${UUID.randomUUID()}"
        val topic2 = "test-shared-unsub-${UUID.randomUUID()}"

        withClusterAndLogs(listOf(topic1, topic2)) { _, logs ->
            val (log1, log2) = logs
            val listener1 = TrackingListener()
            val listener2 = TrackingListener()

            val job1 = launch { log1.openGroupSubscription(listener1) }
            val job2 = launch { log2.openGroupSubscription(listener2) }

            while (!listener1.isAssigned || !listener2.isAssigned) delay(100.milliseconds)

            job1.cancelAndJoin()

            log2.appendMessage(txMessage(3))
            while (listener2.records.isEmpty()) delay(100.milliseconds)

            assertEquals(1, listener2.records.size)
            val msg = listener2.records[0].message
            check(msg is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 3), msg.payload)

            job2.cancelAndJoin()
        }
    }

    @Test
    fun `database can resubscribe after unsubscribing`() = runTest(timeout = 60.seconds) {
        val topic1 = "test-shared-resub-${UUID.randomUUID()}"

        withClusterAndLogs(listOf(topic1)) { _, logs ->
            val log1 = logs[0]

            val listener1 = TrackingListener()
            val job1 = launch { log1.openGroupSubscription(listener1) }
            while (!listener1.isAssigned) delay(100.milliseconds)

            log1.appendMessage(txMessage(1))
            while (listener1.records.isEmpty()) delay(100.milliseconds)
            val firstOffset = listener1.records[0].logOffset

            job1.cancelAndJoin()
            while (listener1.revokedPartitions.isEmpty()) delay(100.milliseconds)

            val listener2 = TrackingListener(afterMsgId = firstOffset)
            val job2 = launch { log1.openGroupSubscription(listener2) }
            while (!listener2.isAssigned) delay(100.milliseconds)

            log1.appendMessage(txMessage(2))
            while (listener2.records.isEmpty()) delay(100.milliseconds)

            assertEquals(1, listener2.records.size)
            val msg = listener2.records[0].message
            check(msg is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 2), msg.payload)

            job2.cancelAndJoin()
        }
    }

    @Test
    fun `shared consumer survives all databases unsubscribing then new one subscribing`() =
        runTest(timeout = 60.seconds) {
            val topic1 = "test-shared-drain-${UUID.randomUUID()}"
            val topic2 = "test-shared-drain-${UUID.randomUUID()}"

            withClusterAndLogs(listOf(topic1, topic2)) { _, logs ->
                val (log1, log2) = logs

                val listener1 = TrackingListener()
                val job1 = launch { log1.openGroupSubscription(listener1) }
                while (!listener1.isAssigned) delay(100.milliseconds)

                job1.cancelAndJoin()

                val listener2 = TrackingListener()
                val job2 = launch { log2.openGroupSubscription(listener2) }
                while (!listener2.isAssigned) delay(100.milliseconds)

                log2.appendMessage(txMessage(4))
                while (listener2.records.isEmpty()) delay(100.milliseconds)

                assertEquals(1, listener2.records.size)

                job2.cancelAndJoin()
            }
        }
}
