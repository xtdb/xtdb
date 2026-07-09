package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
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

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
        nodeBase.close()
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

    private fun logProcessor(
        dbStorage: DatabaseStorage,
        dbState: DatabaseState,
        watchers: Watchers,
        blockUploader: BlockUploader,
        scope: CoroutineScope,
    ) = LogProcessor(
        allocator, nodeBase, mockk(relaxed = true),
        dbStorage, dbState, watchers, blockUploader,
        mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
        externalSourceFactory = null,
        scope = scope,
    )

    @Test
    fun `fresh node starts up with epoch 0`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val dbState = dbState()
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null, null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(dbStorage, dbState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // Teardown: cancel+join the scope reaps the subscription and the live term, then free it.
        scope.coroutineContext.job.cancelAndJoin()
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
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null, null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(dbStorage, dbState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // Teardown: cancel+join the scope reaps the subscription and the live term, then free it.
        scope.coroutineContext.job.cancelAndJoin()
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
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null, null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Pre-populate the replica log with a transaction
        val replicaProducer = replicaLog.openAtomicProducer("setup")
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))
        replicaProducer.close()

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(dbStorage, dbState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // wait for the follower→leader transition to complete (runs on Dispatchers.Default)
        watchers.awaitTx(1)

        verify { liveIndex.commitTx(any(), any()) }

        // Teardown: cancel+join the scope reaps the subscription and the live term, then free it.
        scope.coroutineContext.job.cancelAndJoin()
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
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null, null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Pre-populate the replica log
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(dbStorage, dbState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        watchers.awaitTx(1)

        verify { liveIndex.commitTx(any(), any()) }

        // Teardown: cancel+join the scope reaps the subscription and the live term, then free it.
        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }
}
