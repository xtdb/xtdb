package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import xtdb.api.log.*
import xtdb.api.log.Log.Companion.tailAll
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import xtdb.util.closeOnCatch
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
        dbStorage: DatabaseStorage
    ) =
        object : LogProcessor.ProcessorFactory {
            override fun openLeaderSystem(
                replicaProducer: Log.AtomicProducer<ReplicaMessage>,
                afterMsgId: MessageId
            ): LogProcessor.LeaderSystem {
                val watchers = Watchers(-1)
                val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
                val blockFinisher = BlockFinisher(dbStorage, dbState, compactor, null)
                return LeaderLogProcessor(
                    allocator, dbStorage, replicaProducer,
                    dbState, mockk(relaxed = true), watchers,
                    emptySet(), null, blockFinisher
                ).closeOnCatch { proc ->
                    val sub = dbStorage.sourceLog.tailAll(afterMsgId, proc)
                    object : LogProcessor.LeaderSystem {
                        override val pendingBlock: PendingBlock? get() = proc.pendingBlock
                        override fun close() {
                            sub.close()
                            proc.close()
                        }
                    }
                }
            }

            override fun openTransition(
                replicaProducer: Log.AtomicProducer<ReplicaMessage>, replicaWatchers: Watchers
            ): LogProcessor.TransitionProcessor =
                TransitionLogProcessor(
                    allocator, bufferPool, dbState, dbState.liveIndex,
                    BlockFinisher(dbStorage, dbState, mockk(relaxed = true), null),
                    replicaProducer, replicaWatchers, Watchers(-1), dbCatalog = null
                )

            override fun openFollower(replicaWatchers: Watchers, pendingBlock: PendingBlock?): LogProcessor.FollowerProcessor =
                FollowerLogProcessor(
                    allocator, bufferPool, dbState,
                    mockk(relaxed = true), Watchers(-1), replicaWatchers, null, pendingBlock
                )
        }

    @Test
    fun `fresh node starts up with epoch 0`() {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val watchers = Watchers(-1)
        val blockFinisher = BlockFinisher(dbStorage, dbState, mockk(relaxed = true), null)

        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage),
            dbStorage, dbState, watchers, blockFinisher
        )

        // openGroupConsumer triggers onPartitionsAssigned synchronously
        val consumer = sourceLog.openGroupConsumer(logProc)

        consumer.close()
        logProc.close()
        watchers.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `fresh node starts up with non-zero epoch`() {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 1)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 1)
        val bufferPool = mockBufferPool(epoch = 1)
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val watchers = Watchers(-1)
        val blockFinisher = BlockFinisher(dbStorage, dbState, mockk(relaxed = true), null)

        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage),
            dbStorage, dbState, watchers, blockFinisher
        )

        val consumer = sourceLog.openGroupConsumer(logProc)

        consumer.close()
        logProc.close()
        watchers.close()
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
        val watchers = Watchers(-1)
        val blockFinisher = BlockFinisher(dbStorage, dbState, mockk(relaxed = true), null)

        // Pre-populate the replica log with a transaction
        val replicaProducer = replicaLog.openAtomicProducer("setup")
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap())).await()
        replicaProducer.close()

        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage),
            dbStorage, dbState, watchers, blockFinisher
        )

        val consumer = sourceLog.openGroupConsumer(logProc)

        // The transition should have replayed the ResolvedTx
        verify { liveIndex.importTx(any()) }

        consumer.close()
        logProc.close()
        watchers.close()
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
        val watchers = Watchers(-1)
        val blockFinisher = BlockFinisher(dbStorage, dbState, mockk(relaxed = true), null)

        // Pre-populate the replica log
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap())).await()

        val logProc = LogProcessor(
            procFactory(allocator, bufferPool, dbState, dbStorage),
            dbStorage, dbState, watchers, blockFinisher
        )

        val consumer = sourceLog.openGroupConsumer(logProc)

        verify { liveIndex.importTx(any()) }

        consumer.close()
        logProc.close()
        watchers.close()
        sourceLog.close()
        replicaLog.close()
    }
}
