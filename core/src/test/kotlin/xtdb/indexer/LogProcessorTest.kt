package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import xtdb.api.log.*
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.InstantSource
import java.util.concurrent.TimeUnit

@Timeout(10, unit = TimeUnit.SECONDS)
class LogProcessorTest {

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

    private fun dbState(name: String = "test-db", liveIndex: LiveIndex = mockk(relaxed = true)) =
        DatabaseState(
            name,
            BlockCatalog(name, null),
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )

    private fun procFactory(
        allocator: RootAllocator,
        bufferPool: BufferPool,
        dbState: DatabaseState,
        dbStorage: DatabaseStorage,
        watchers: Watchers,
    ) =
        object : LogProcessor.ProcessorFactory {
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
                    override fun close() = leaderProc.close()
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
    fun `fresh node starts up with epoch 0`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val job = scope.launch { sourceLog.openGroupSubscription(logProc) }

        job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `fresh node starts up with non-zero epoch`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 1)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 1)
        val bufferPool = mockBufferPool(epoch = 1)
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val job = scope.launch { sourceLog.openGroupSubscription(logProc) }

        job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `leader replays existing replica messages during transition`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
        }
        val dbState = dbState(liveIndex = liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        // Pre-populate the replica log with a transaction
        val replicaProducer = replicaLog.openAtomicProducer("setup")
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))
        replicaProducer.close()

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val job = scope.launch { sourceLog.openGroupSubscription(logProc) }

        // wait for the follower→leader transition to complete (runs on Dispatchers.Default)
        watchers.awaitTx(1)

        verify { liveIndex.importTx(any()) }

        runBlocking { job.cancelAndJoin() }
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `leader replays existing replica messages with non-zero epoch`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 1)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 1)
        val bufferPool = mockBufferPool(epoch = 1)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
        }
        val dbState = dbState(liveIndex = liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val watchers = Watchers(-1)

        // Pre-populate the replica log
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage, watchers),
            dbStorage, dbState, blockUploader, scope
        )

        val job = scope.launch { sourceLog.openGroupSubscription(logProc) }

        watchers.awaitTx(1)

        verify { liveIndex.importTx(any()) }

        runBlocking { job.cancelAndJoin() }
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }
}
