package xtdb.api.log

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import org.apache.arrow.memory.RootAllocator
import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.api.storage.Storage
import xtdb.indexer.*
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.InstantSource
import java.util.concurrent.TimeUnit

private class FenceableProducer<M>(private val delegate: Log.AtomicProducer<M>) : Log.AtomicProducer<M> {
    @Volatile
    var fenced = false

    override fun isProducerFenced(e: Throwable) = KafkaCluster.AtomicProducer.isKafkaProducerFenced(e)

    override fun openTx(): Log.AtomicProducer.Tx<M> {
        if (fenced) throw ProducerFencedException("simulated fencing")
        val tx = delegate.openTx()
        return object : Log.AtomicProducer.Tx<M> {
            override fun appendMessage(message: M) = tx.appendMessage(message)
            override fun commit() {
                if (fenced) throw ProducerFencedException("simulated fencing")
                tx.commit()
            }

            override fun abort() = tx.abort()
            override fun close() = tx.close()
        }
    }

    override fun close() = delegate.close()
}

private class FenceableReplicaLog(
    private val delegate: InMemoryLog<ReplicaMessage>
) : Log<ReplicaMessage> by delegate {
    var fenceableProducer: FenceableProducer<ReplicaMessage>? = null
        private set

    override fun openAtomicProducer(transactionalId: String): Log.AtomicProducer<ReplicaMessage> {
        val producer = FenceableProducer(delegate.openAtomicProducer(transactionalId))
        fenceableProducer = producer
        return producer
    }
}

@Timeout(30, unit = TimeUnit.SECONDS)
class KafkaProducerFencingTest {

    private lateinit var allocator: RootAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    private fun mockBufferPool(epoch: Int = 0) =
        mockk<BufferPool>(relaxed = true) { every { this@mockk.epoch } returns epoch }

    private fun dbState(name: String = "test-db") =
        DatabaseState(
            name,
            BlockCatalog(name, null),
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            mockk(relaxed = true)
        )

    private fun procFactory(
        bufferPool: BufferPool,
        dbState: DatabaseState,
        dbStorage: DatabaseStorage,
        watchers: Watchers,
    ) = object : LogProcessor.ProcessorFactory {
        override fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.LeaderSystem {
            val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
            val blockUploader = BlockUploader(dbStorage, dbState, compactor, null)
            val leaderProc = LeaderLogProcessor(
                allocator, dbStorage, replicaProducer,
                dbState, mockk(relaxed = true), watchers,
                emptySet(), null, blockUploader, afterSourceMsgId, afterReplicaMsgId
            )
            return object : LogProcessor.LeaderSystem {
                override val proc get() = leaderProc
                override fun isProducerFenced(e: Throwable) = replicaProducer.isProducerFenced(e)
                override fun close() { leaderProc.close(); replicaProducer.close() }
            }
        }

        override fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.TransitionProcessor =
            TransitionLogProcessor(
                allocator, bufferPool, dbState, dbState.liveIndex,
                BlockUploader(dbStorage, dbState, mockk(relaxed = true), null),
                replicaProducer, watchers, dbCatalog = null,
                afterSourceMsgId = afterSourceMsgId,
                afterReplicaMsgId = afterReplicaMsgId,
            )

        override fun openFollower(
            pendingBlock: PendingBlock?,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.FollowerProcessor =
            FollowerLogProcessor(
                allocator, bufferPool, dbState,
                mockk(relaxed = true), watchers, null, pendingBlock,
                afterSourceMsgId, afterReplicaMsgId,
            )
    }

    @Test
    fun `fencing during leader transition reverts to follower`() = runBlocking {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val dbState = dbState()

        val alwaysFencedReplicaLog = object : Log<ReplicaMessage> by replicaLog {
            override fun openAtomicProducer(transactionalId: String): Log.AtomicProducer<ReplicaMessage> {
                val producer = FenceableProducer(replicaLog.openAtomicProducer(transactionalId))
                producer.fenced = true
                return producer
            }
        }

        val dbStorage = DatabaseStorage(sourceLog, alwaysFencedReplicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val tailSpec = logProc.onPartitionsAssigned(listOf(0))
        assertEquals(null, tailSpec)

        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    private fun triesAddedMessage() = SourceMessage.TriesAdded(Storage.VERSION, 0, emptyList())

    @Test
    fun `fencing during processRecords does not poison watchers`() = runBlocking {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val fenceableReplicaLog = FenceableReplicaLog(replicaLog)
        val bufferPool = mockBufferPool()
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, fenceableReplicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val job = scope.launch { sourceLog.openGroupSubscription(logProc) }

        val firstMsgId = sourceLog.appendMessage(triesAddedMessage()).msgId
        watchers.awaitSource(firstMsgId)

        fenceableReplicaLog.fenceableProducer!!.fenced = true
        sourceLog.appendMessage(triesAddedMessage())

        // InMemoryLog doesn't catch StepDownException so the subscription dies,
        // but watchers should not be poisoned — LeaderLogProcessor skips notifyError for fencing.
        job.join()

        assertEquals(null, watchers.exception)

        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `non-fencing exceptions poison watchers`() = runBlocking {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val fenceableReplicaLog = FenceableReplicaLog(replicaLog)
        val bufferPool = mockBufferPool()
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, fenceableReplicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val job = scope.launch { sourceLog.openGroupSubscription(logProc) }

        val firstMsgId = sourceLog.appendMessage(triesAddedMessage()).msgId
        watchers.awaitSource(firstMsgId)

        sourceLog.appendMessage(SourceMessage.Tx(ByteArray(0), null, java.time.ZoneId.of("UTC"), null, null))

        job.join()

        assertNotEquals(null, watchers.exception)

        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `isProducerFenced recognizes all fencing exception types`() {
        val producer = FenceableProducer(InMemoryLog<ReplicaMessage>(InstantSource.system(), 0).openAtomicProducer("test"))

        assertTrue(producer.isProducerFenced(ProducerFencedException("fenced")))
        assertTrue(producer.isProducerFenced(InvalidProducerEpochException("stale epoch")))
        assertTrue(producer.isProducerFenced(OutOfOrderSequenceException("sequence gap")))
        assertTrue(producer.isProducerFenced(RuntimeException("wrapper", ProducerFencedException("nested"))))
        assertFalse(producer.isProducerFenced(RuntimeException("unrelated")))

        producer.close()
    }
}
