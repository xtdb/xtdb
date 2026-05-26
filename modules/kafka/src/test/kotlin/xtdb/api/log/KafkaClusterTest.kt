package xtdb.api.log

import com.google.protobuf.ByteString
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.Xtdb
import xtdb.api.log.Log.*
import xtdb.api.storage.Storage
import xtdb.cache.DiskCache
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.util.MsgIdUtil
import xtdb.util.asPath
import xtdb.util.closeAll
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Duration
import java.util.*
import java.util.Collections.synchronizedList
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
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

    private class RacingListener : SubscriptionListener<SourceMessage> {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val records = CopyOnWriteArrayList<Record<SourceMessage>>()

        override suspend fun onPartitionsAssigned(partitions: Collection<Int>): TailSpec<SourceMessage> {
            entered.complete(Unit)
            release.await()
            return TailSpec(-1L, RecordProcessor { recs -> records.addAll(recs) })
        }

        override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {}
    }

    @Test
    fun `wakeup during seek does not skip seek (regression for #5633)`() = runTest(timeout = 10.seconds) {
        val topic1 = "test-seek-race-${UUID.randomUUID()}"
        val topic2 = "test-seek-race-${UUID.randomUUID()}"

        withClusterAndLogs(listOf(topic1, topic2)) { _, logs ->
            val (log1, log2) = logs
            val listener1 = RacingListener()
            val listener2 = RacingListener()

            val job1 = launch { log1.openGroupSubscription(listener1) }
            listener1.entered.await()

            // second register() arms consumer.wakeup() while topic1's callback is parked.
            val job2 = launch { log2.openGroupSubscription(listener2) }
            yield()

            listener1.release.complete(Unit)

            listener2.entered.await()
            listener2.release.complete(Unit)

            log1.appendMessage(txMessage(1))
            log2.appendMessage(txMessage(2))

            // an inner withTimeout would use the TestScope's virtual clock and trip before
            // records flow; rely on the outer real-time runTest timeout instead.
            while (listener1.records.isEmpty() || listener2.records.isEmpty()) delay(50.milliseconds)

            job1.cancelAndJoin()
            job2.cancelAndJoin()
        }
    }

    private class ThrowingOnAssignListener(private val toThrow: Throwable) : SubscriptionListener<SourceMessage> {
        override suspend fun onPartitionsAssigned(partitions: Collection<Int>): TailSpec<SourceMessage> {
            throw toThrow
        }

        override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {}
    }

    @Test
    fun `poll loop failure evicts all subscriptions with cause and unblocks shutdown`() = runTest(timeout = 30.seconds) {
        val topic1 = "test-poll-fail-${UUID.randomUUID()}"
        val topic2 = "test-poll-fail-${UUID.randomUUID()}"

        val boom = RuntimeException("listener boom")
        val healthy = TrackingListener()
        val throwing = ThrowingOnAssignListener(boom)

        val caughtHealthy = AtomicReference<Throwable?>(null)
        val caughtThrowing = AtomicReference<Throwable?>(null)

        withClusterAndLogs(listOf(topic1, topic2)) { _, logs ->
            val (logHealthy, logThrowing) = logs

            // register the healthy subscriber first and wait for it to be assigned —
            // guarantees it's in the subscriptions map when the throwing one tanks the poll loop
            val jobHealthy = launch(SupervisorJob()) {
                try { logHealthy.openGroupSubscription(healthy) }
                catch (e: Throwable) { caughtHealthy.set(e) }
            }
            while (!healthy.isAssigned) delay(100.milliseconds)

            val jobThrowing = launch(SupervisorJob()) {
                try { logThrowing.openGroupSubscription(throwing) }
                catch (e: Throwable) { caughtThrowing.set(e) }
            }

            // both subscribers should surface a failure (no hang) — the outer runTest timeout
            // is the regression canary for the pre-fix shutdown-hang behaviour
            jobHealthy.join()
            jobThrowing.join()
        }

        val exThrowing = caughtThrowing.get()
            ?: error("throwing subscriber must surface a failure, not hang")
        val exHealthy = caughtHealthy.get()
            ?: error("healthy subscriber must surface a failure when the poll loop dies, not hang")

        assertTrue(generateSequence<Throwable>(exThrowing) { it.cause }.any { it === boom },
            "throwing subscriber must see the listener exception in its cause chain, got: $exThrowing")
        assertTrue(generateSequence<Throwable>(exHealthy) { it.cause }.any { it === boom },
            "healthy subscriber must see the listener exception in its cause chain, got: $exHealthy")
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
    fun `node configured with Kafka cluster under remotes round-trips a tx`() {
        val topicName = "test-remotes-${UUID.randomUUID()}"

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            remote("my-kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("my-kafka", topicName))
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('a', 1)")
                    stmt.executeQuery("SELECT _id, x FROM foo").use { rs ->
                        assertTrue(rs.next())
                        assertEquals("a", rs.getString("_id"))
                        assertEquals(1, rs.getInt("x"))
                    }
                }
            }
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

    @Test
    @Timeout(value = 30, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `DETACH on Kafka-backed secondary completes promptly (#5613)`() {
        val primaryTopic = "kafka-detach-primary-${UUID.randomUUID()}"
        val secondaryTopic = "kafka-detach-secondary-${UUID.randomUUID()}"

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", primaryTopic))
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        ATTACH DATABASE secondary WITH ${'$'}${'$'}
                            log: !Kafka
                              cluster: kafka
                              topic: $secondaryTopic
                        ${'$'}${'$'}""".trimIndent()
                    )
                    stmt.execute("DETACH DATABASE secondary")
                }
            }
        }
    }

    private suspend fun Log<SourceMessage>.seedAndTruncate(topic: String, count: Int, truncateBefore: Long) {
        repeat(count) { i -> appendMessage(txMessage((i + 1).toByte())) }
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to container.bootstrapServers)).use { admin ->
            admin.deleteRecords(
                mapOf(TopicPartition(topic, 0) to RecordsToDelete.beforeOffset(truncateBefore))
            ).all().get()
        }
    }

    @Test
    @Timeout(value = 30, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `tailAll fails when its anchor offset has been truncated`() = runBlocking {
        val topic = "trunc-anchored-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())
        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }
        val caught = AtomicReference<Throwable?>(null)

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topic)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.seedAndTruncate(topic, count = 5, truncateBefore = 3L)

                        // Anchor inside the now-truncated prefix — seek lands below the earliest available offset.
                        val anchorInTruncatedPrefix = MsgIdUtil.offsetToMsgId(0, 1L)
                        val job = launch(SupervisorJob()) {
                            try { log.tailAll(anchorInTruncatedPrefix, subscriber) }
                            catch (e: Throwable) { caught.set(e) }
                        }
                        val completed = withTimeoutOrNull(3.seconds) { job.join() } != null
                        job.cancelAndJoin()

                        assertTrue(completed, "tailAll silently polled past the truncation — should have thrown")
                    }
            }

        assertEquals(0, synchronized(msgs) { msgs.flatten().size },
            "received records past a truncated anchor — silent skip is data loss")
        assertNotNull(caught.get(), "tailAll must throw, not silently auto-reset")
    }

    @Test
    @Timeout(value = 30, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `openGroupSubscription surfaces error when anchor truncated`() = runBlocking {
        val topic = "trunc-grp-${UUID.randomUUID()}"
        val anchorInTruncatedPrefix = MsgIdUtil.offsetToMsgId(0, 1L)
        val caught = AtomicReference<Throwable?>(null)
        val listener = object : SubscriptionListener<SourceMessage> {
            override suspend fun onPartitionsAssigned(partitions: Collection<Int>): TailSpec<SourceMessage> =
                TailSpec(afterMsgId = anchorInTruncatedPrefix, processor = RecordProcessor { _ -> })
            override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {}
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topic)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.seedAndTruncate(topic, count = 5, truncateBefore = 3L)

                        val job = launch(SupervisorJob()) {
                            try { log.openGroupSubscription(listener) }
                            catch (e: Throwable) { caught.set(e) }
                        }
                        val completed = withTimeoutOrNull(5.seconds) { job.join() } != null
                        job.cancelAndJoin()

                        assertTrue(completed, "openGroupSubscription silently polled past the truncation — should have thrown")
                    }
            }

        assertNotNull(caught.get(), "openGroupSubscription must surface OffsetOutOfRange to its subscriber")
    }

    @Test
    @Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `node surfaces error on restart when replica log truncated past stored boundary`(
        @TempDir storageDir: Path, @TempDir cacheDir1: Path, @TempDir cacheDir2: Path,
    ) {
        val sourceTopic = "trunc-node-${UUID.randomUUID()}"
        val replicaTopic = "$sourceTopic-replica"

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir1))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", sourceTopic))
            storage(Storage.local(storageDir))
            compactor { threads(0) }
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('a', 1)")
                }
            }
            val cat = (node as Xtdb.XtdbInternal).dbCatalog
            cat.primary.sendFlushBlockMessage()
            cat.syncAll(Duration.ofSeconds(10))
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('b', 2)")
                }
            }
        }

        // Truncate the entire replica log — the earliest available offset is now past the persisted boundary.
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to container.bootstrapServers)).use { admin ->
            val tp = TopicPartition(replicaTopic, 0)
            val endOffset = admin.listOffsets(mapOf(tp to org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get()[tp]!!.offset()
            admin.deleteRecords(mapOf(tp to RecordsToDelete.beforeOffset(endOffset))).all().get()
        }

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir2))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", sourceTopic))
            storage(Storage.local(storageDir))
            compactor { threads(0) }
        }.use { node ->
            val primary = (node as Xtdb.XtdbInternal).dbCatalog.primary
            val deadline = System.currentTimeMillis() + 30_000
            while (primary.ingestionError == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(100)
            }
            assertNotNull(primary.ingestionError,
                "node must surface IngestionStoppedException when replica log truncated past stored boundary")
        }
    }

    @Test
    @Timeout(value = 90, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `secondary failure does not affect primary and can still be detached`(
        @TempDir primaryStorage: Path,
        @TempDir secondaryStorage: Path,
        @TempDir cacheDir1: Path,
        @TempDir cacheDir2: Path,
    ) {
        val primaryTopic = "trunc-secondary-primary-${UUID.randomUUID()}"
        val secondaryTopic = "trunc-secondary-${UUID.randomUUID()}"
        val secondaryReplicaTopic = "$secondaryTopic-replica"

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir1))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", primaryTopic))
            storage(Storage.local(primaryStorage))
            compactor { threads(0) }
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        ATTACH DATABASE secondary WITH ${'$'}${'$'}
                            log: !Kafka
                              cluster: kafka
                              topic: $secondaryTopic
                            storage: !Local
                              path: "${secondaryStorage.toString().replace("\\", "/")}"
                        ${'$'}${'$'}""".trimIndent()
                    )
                }
            }

            val cat = (node as Xtdb.XtdbInternal).dbCatalog
            node.createConnectionBuilder().database("secondary").build().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('a', 1)")
                }
            }
            cat["secondary"]!!.sendFlushBlockMessage()
            cat.syncAll(Duration.ofSeconds(10))
            node.createConnectionBuilder().database("secondary").build().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('b', 2)")
                }
            }
        }

        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to container.bootstrapServers)).use { admin ->
            val tp = TopicPartition(secondaryReplicaTopic, 0)
            val endOffset = admin.listOffsets(mapOf(tp to org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get()[tp]!!.offset()
            admin.deleteRecords(mapOf(tp to RecordsToDelete.beforeOffset(endOffset))).all().get()
        }

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir2))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", primaryTopic))
            storage(Storage.local(primaryStorage))
            compactor { threads(0) }
        }.use { node ->
            val cat = (node as Xtdb.XtdbInternal).dbCatalog
            val primary = cat.primary
            val deadline = System.currentTimeMillis() + 30_000
            while (cat["secondary"]?.ingestionError == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(100)
            }
            assertNotNull(cat["secondary"]?.ingestionError,
                "secondary must surface IngestionStoppedException when its replica log is truncated past the stored boundary")
            assertEquals(null, primary.ingestionError,
                "primary must remain healthy when an unrelated secondary fails")

            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DETACH DATABASE secondary")
                }
            }
            assertEquals(null, cat["secondary"],
                "DETACH on a failed secondary must complete and remove it from the catalog")
            assertEquals(null, primary.ingestionError,
                "primary must remain healthy after detaching the failed secondary")
        }
    }

    @Test
    @Timeout(value = 30, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `tailAll picks up records appended past a truncated prefix when starting fresh`() = runBlocking {
        val topic = "trunc-fresh-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())
        val subscriber = mockk<RecordProcessor<SourceMessage>> {
            coEvery { processRecords(capture(msgs)) } returns Unit
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topic)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.seedAndTruncate(topic, count = 20, truncateBefore = 20L)
                        log.appendMessage(txMessage(1))
                        log.appendMessage(txMessage(2))
                        log.appendMessage(txMessage(3))

                        val job = launch { log.tailAll(-1L, subscriber) }
                        try {
                            withTimeoutOrNull(5.seconds) {
                                while (synchronized(msgs) { msgs.flatten().size } < 3) delay(100.milliseconds)
                            }
                        } finally {
                            job.cancelAndJoin()
                        }
                    }
            }

        assertEquals(3, synchronized(msgs) { msgs.flatten().size })
    }

    @Test
    @Timeout(value = 30, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `openGroupSubscription picks up records appended past a truncated prefix when starting fresh`() = runBlocking {
        val topic = "trunc-fresh-grp-${UUID.randomUUID()}"
        val msgs = synchronizedList(mutableListOf<List<Record<SourceMessage>>>())
        val processor = RecordProcessor<SourceMessage> { records -> msgs.add(records) }
        val listener = object : SubscriptionListener<SourceMessage> {
            override suspend fun onPartitionsAssigned(partitions: Collection<Int>): TailSpec<SourceMessage> =
                TailSpec(afterMsgId = -1L, processor = processor)
            override suspend fun onPartitionsRevoked(partitions: Collection<Int>) {}
        }

        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", topic)
                    .openSourceLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.seedAndTruncate(topic, count = 20, truncateBefore = 20L)
                        log.appendMessage(txMessage(1))
                        log.appendMessage(txMessage(2))
                        log.appendMessage(txMessage(3))

                        val job = launch { log.openGroupSubscription(listener) }
                        try {
                            withTimeoutOrNull(5.seconds) {
                                while (synchronized(msgs) { msgs.flatten().size } < 3) delay(100.milliseconds)
                            }
                        } finally {
                            job.cancelAndJoin()
                        }
                    }
            }

        assertEquals(3, synchronized(msgs) { msgs.flatten().size })
    }

    @Test
    @Timeout(value = 90, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `node re-indexes from the truncated prefix on restart without a persisted block`(
        @TempDir storage: Path,
        @TempDir cacheDir1: Path,
        @TempDir cacheDir2: Path,
    ) {
        val sourceTopic = "trunc-restart-${UUID.randomUUID()}"

        val props = mapOf<String, Any>("bootstrap.servers" to container.bootstrapServers, "acks" to "all")
        KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
            repeat(20) { producer.send(ProducerRecord(sourceTopic, ByteArray(8) { 0 })) }
            producer.flush()
        }
        AdminClient.create(props).use { admin ->
            admin.deleteRecords(
                mapOf(TopicPartition(sourceTopic, 0) to RecordsToDelete.beforeOffset(20L))
            ).all().get()
        }

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir1))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", sourceTopic))
            storage(Storage.local(storage))
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('a', 1)")
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('b', 2)")
                }
            }
        }

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir2))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", sourceTopic))
            storage(Storage.local(storage))
        }.use { node ->
            // An INSERT blocks on awaitTx for its own tx, which is processed strictly after
            // session 1's records on the source log — so by the time this returns, the prior
            // session's txs have been re-indexed.
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('c', 3)")
                    stmt.executeQuery("SELECT _id, x FROM foo ORDER BY _id").use { rs ->
                        assertTrue(rs.next()); assertEquals("a", rs.getString("_id")); assertEquals(1, rs.getInt("x"))
                        assertTrue(rs.next()); assertEquals("b", rs.getString("_id")); assertEquals(2, rs.getInt("x"))
                        assertTrue(rs.next()); assertEquals("c", rs.getString("_id")); assertEquals(3, rs.getInt("x"))
                        assertTrue(!rs.next())
                    }
                }
            }
            val primary = (node as Xtdb.XtdbInternal).dbCatalog.primary
            assertEquals(null, primary.ingestionError, "node must replay cleanly from the truncated prefix")
        }
    }

    @Test
    @Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
    fun `node starts cleanly against a pre-truncated topic prefix`(@TempDir cacheDir: Path) {
        val sourceTopic = "trunc-fresh-node-${UUID.randomUUID()}"

        val props = mapOf<String, Any>("bootstrap.servers" to container.bootstrapServers, "acks" to "all")
        KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
            repeat(20) { producer.send(ProducerRecord(sourceTopic, ByteArray(8) { 0 })) }
            producer.flush()
        }
        AdminClient.create(props).use { admin ->
            admin.deleteRecords(
                mapOf(TopicPartition(sourceTopic, 0) to RecordsToDelete.beforeOffset(20L))
            ).all().get()
        }

        Xtdb.openNode {
            server { port = 0 }; flightSql = null
            diskCache(DiskCache.factory(cacheDir))
            logCluster("kafka", KafkaCluster.ClusterFactory(container.bootstrapServers))
            log(KafkaCluster.LogFactory("kafka", sourceTopic))
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSERT INTO foo (_id, x) VALUES ('a', 1)")
                    stmt.executeQuery("SELECT _id, x FROM foo").use { rs ->
                        assertTrue(rs.next())
                        assertEquals("a", rs.getString("_id"))
                        assertEquals(1, rs.getInt("x"))
                    }
                }
            }
            val primary = (node as Xtdb.XtdbInternal).dbCatalog.primary
            assertEquals(null, primary.ingestionError,
                "a fresh node must not error when the topic's earliest offset is past 0")
        }
    }
}
