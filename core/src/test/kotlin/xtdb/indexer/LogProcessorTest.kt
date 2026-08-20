package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows
import xtdb.api.IndexerConfig
import xtdb.api.error.Incorrect
import org.junit.jupiter.api.Timeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.api.log.*
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
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

    private fun newPartitionState(name: String = "test-db", liveIndex: LiveIndex = mockk(relaxed = true)) =
        PartitionState(
            BlockCatalog(null),
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )

    private fun logProcessor(
        partitionStorage: PartitionStorage,
        partitionState: PartitionState,
        watchers: Watchers,
        blockUploader: BlockUploader,
        scope: CoroutineScope,
    ) = LogProcessor(
        allocator, nodeBase, mockk(relaxed = true),
        partitionStorage, partitionState, "test-db", watchers, blockUploader,
        mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
        externalSourceFactory = null,
        scope = scope,
        flushTimeout = IndexerConfig().flushDuration,
    )

    @Test
    fun `fresh node starts up with epoch 0`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

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
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // Teardown: cancel+join the scope reaps the subscription and the live term, then free it.
        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    // A leader's election counter is only monotonic within one incarnation of the mechanism that
    // elects it: Kafka deletes an idle consumer group, and the local logs' counter dies with the
    // process. The next pair covers both sides of a counter that has restarted below the terms
    // already on the replica log — see LeaderTerm and #5817.

    @Test
    fun `refuses to lead when the election counter has regressed below the replica log`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        // a previous incarnation of the counter reached 9
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 9)))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        // ...so the fresh counter's first term, 0.1, is one every reader would discard
        val subscription = scope.async { sourceLog.openGroupSubscription(logProc) }
        val e = assertThrows<Incorrect> { subscription.await() }
        assertTrue(
            e.message!!.contains("termEpoch"),
            "the refusal names the knob that fixes it, was: ${e.message}"
        )

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `leads once the term epoch is raised past the regressed counter`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0, termEpoch = 1)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val liveIndex = mockk<LiveIndex>(relaxed = true) { every { latestCompletedTx } returns null }
        val partitionState = newPartitionState(liveIndex = liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 9)))
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // term 1.1 outranks 0.9, so the transition goes through and replays the log
        watchers.awaitTx(1)
        verify { liveIndex.commitTx(any(), any()) }

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
        val partitionState = newPartitionState(liveIndex = liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        // Pre-populate the replica log with a transaction
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

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
        val partitionState = newPartitionState(liveIndex = liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        // Pre-populate the replica log
        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap()))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        watchers.awaitTx(1)

        verify { liveIndex.commitTx(any(), any()) }

        // Teardown: cancel+join the scope reaps the subscription and the live term, then free it.
        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a term already on the replica log still fences after a demote`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        val highTerm = LeaderTerm.of(0, 9)
        logProc.launchTransition(0, highTerm).await()
        logProc.commitLeader(0)

        // Nothing has been flushed, so the persisted boundary still carries no term at all — which is
        // what a fence re-seeded on the new follower would fall back to.
        logProc.demoteLeader(0)

        assertEquals(
            highTerm, partitionState.termFence.highest,
            "the demote does not lower what the log has been seen to reach"
        )

        assertThrows<Incorrect> { logProc.launchTransition(0, LeaderTerm.of(0, 5)).await() }

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }
}
