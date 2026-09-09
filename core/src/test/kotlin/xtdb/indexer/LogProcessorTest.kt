package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import xtdb.api.IndexerConfig
import xtdb.block.proto.block
import xtdb.api.error.Conflict
import org.junit.jupiter.api.Timeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.api.log.*
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.storage.Storage
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.BufferPool
import xtdb.util.closeAll
import java.time.Instant
import java.time.InstantSource
import java.util.concurrent.TimeUnit

@Timeout(10, unit = TimeUnit.SECONDS)
class LogProcessorTest {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

    // Each test cancel-and-joins its own scope and closes its processor, so everything here is
    // quiescent by teardown — and freed before `allocator`, which the live indexes are children of.
    private val partitionStates = mutableListOf<PartitionState>()

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDown() {
        partitionStates.closeAll()
        allocator.close()
        nodeBase.close()
    }

    private fun mockBufferPool(epoch: Int = 0) =
        mockk<BufferPool>(relaxed = true) { every { this@mockk.epoch } returns epoch }

    private fun newPartitionState(boundaryTermId: Long? = null): PartitionState {
        val tableCatalog = boundaryTermId
            ?.let { TableCatalog(mockBufferPool(), block { blockIndex = 0; termId = it }) }
            ?: TableCatalog(mockBufferPool())
        val trieCatalog = createTrieCatalog()

        return PartitionState(tableCatalog, trieCatalog, LiveIndex.open(allocator, tableCatalog, trieCatalog))
            .also { partitionStates += it }
    }

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
        externalSource = null,
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
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

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
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

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
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // a previous incarnation of the counter reached 9
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 9)))

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        // ...so the fresh counter's first term, 0.1, is one every reader would discard
        val subscription = scope.async { sourceLog.openGroupSubscription(logProc) }
        val e = assertThrows<Conflict> { subscription.await() }
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
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 9)))
        replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(
                1, java.time.Instant.now(), true, null, emptyMap(), termId = LeaderTerm.of(0, 9)
            )
        )

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // term 1.1 outranks 0.9, so the transition goes through and replays the log
        watchers.awaitTx(1)
        assertEquals(
            1L, partitionState.liveIndex.latestCompletedTx?.txId,
            "the replayed tx is committed into the live index"
        )

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
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Pre-populate the replica log with a transaction
        replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(
                1, java.time.Instant.now(), true, null, emptyMap(), termId = LeaderTerm.of(0, 1)
            )
        )

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        // wait for the follower→leader transition to complete (runs on Dispatchers.Default)
        watchers.awaitTx(1)

        assertEquals(
            1L, partitionState.liveIndex.latestCompletedTx?.txId,
            "the replayed tx is committed into the live index"
        )

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
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Pre-populate the replica log
        replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(
                1, java.time.Instant.now(), true, null, emptyMap(), termId = LeaderTerm.of(0, 1)
            )
        )

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        scope.launch { sourceLog.openGroupSubscription(logProc) }

        watchers.awaitTx(1)

        assertEquals(
            1L, partitionState.liveIndex.latestCompletedTx?.txId,
            "the replayed tx is committed into the live index"
        )

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
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        val highTerm = LeaderTerm.of(0, 9)
        logProc.transitionToLeader(0, highTerm).await()

        // Nothing has been flushed, so the persisted boundary still carries no term at all — which is
        // what a fence re-seeded on the new follower would fall back to.
        logProc.demoteLeader(0)

        assertEquals(
            highTerm, logProc.termFence.highestSeen,
            "the demote does not lower what the log has been seen to reach"
        )

        assertThrows<Conflict> { logProc.transitionToLeader(0, LeaderTerm.of(0, 5)).await() }

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `refuses a leader term the persisted boundary has moved past`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()

        // the election counter is back to 1 — a Kafka group deleted while the cluster was down, or a
        // local log's process restarting — while the last block was cut at 9
        val partitionState = newPartitionState(boundaryTermId = LeaderTerm.of(0, 9))
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        assertThrows<Conflict> { logProc.termFence.checkUnfenced(LeaderTerm.of(0, 1)) }
        assertDoesNotThrow("bumping the term epoch clears the regression") {
            logProc.termFence.checkUnfenced(LeaderTerm.of(1, 1))
        }

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `the reader discards a fenced record, still advancing the consume position`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        val leader = replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = LeaderTerm.of(0, 2))
        )
        val superseded = replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), srcMsgId = 2, termId = LeaderTerm.of(0, 1))
        )

        logProc.awaitReplicaMsg(superseded.msgId)

        assertEquals(1L, watchers.latestTxId, "the superseded leader's tx was never applied")
        assertEquals(LeaderTerm.of(0, 2), logProc.termFence.highestSeen)
        assertTrue(leader.msgId < superseded.msgId)

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a block closes even once the fence has moved past the term that cut it`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        every { bufferPool.getByteArray(TableCatalog.blockFilePath(0)) } returns block { blockIndex = 0 }.toByteArray()

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        val cutter = LeaderTerm.of(0, 4)
        replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 0, termId = cutter))
        // a claim landing between the boundary and its upload is what moves the fence past `cutter`,
        // and under self-election it lands there routinely
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 5)))
        val uploaded = replicaLog.appendMessage(
            ReplicaMessage.BlockUploaded(Storage.VERSION, 0, 0, 0, emptyList(), termId = cutter)
        )

        logProc.awaitReplicaMsg(uploaded.msgId)

        assertEquals(
            0L, partitionState.tableCatalog.currentBlockIndex,
            "b0 is closed, so the follower is no longer buffering behind it"
        )
        assertEquals(
            LeaderTerm.of(0, 5), logProc.termFence.highestSeen,
            "the claim it held back still folded, so every reader agrees on what the log has reached"
        )

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a promotion finishes the block it inherits and applies what was held behind it`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        // the leader that cut b0 died before uploading it, so the follower is still holding the block —
        // and the tx behind it, unfolded. That tx carries the boundary's own source position: the block
        // cut pauses resolution, so nothing the leader resolves can land between a boundary and its
        // upload, and a held record that moved the source watermark on would walk it backwards when the
        // upload is read back at the boundary's position.
        val cutter = LeaderTerm.of(0, 4)
        replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
        replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = cutter)
        )

        val incoming = LeaderTerm.of(0, 5)
        logProc.transitionToLeader(0, incoming).await()

        assertEquals(
            0L, partitionState.tableCatalog.currentBlockIndex,
            "the incoming leader finished the block its predecessor left open"
        )
        watchers.awaitTx(1)
        assertEquals(
            incoming, logProc.termFence.highestSeen,
            "the held records folded on replay, up to this leader's own claim"
        )

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a promotion resigns on a superseding term the follower was holding`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        // b0 stays open, so everything after it is held and the fence stays at the boundary's term —
        // which is what lets the claim below pass its own unfenced check
        replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 0, termId = LeaderTerm.of(0, 4)))
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 9)))

        assertThrows<LeaderSupersededException> {
            logProc.transitionToLeader(0, LeaderTerm.of(0, 5)).await()
        }

        assertNull(
            watchers.exception,
            "being superseded is not an ingestion fault, so the database stays queryable"
        )
        assertEquals(
            LeaderTerm.of(0, 4), logProc.termFence.highestSeen,
            "the held records were never folded, so nothing was applied at the superseding term either"
        )

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a catch-up await fails when the replica tail has stopped short of its target`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // The database scope carries a handler in production; without one here the tail's rethrow reaches
        // runTest's uncaught-exception collector and fails the test from outside its assertions.
        val scope = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, _ -> })
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 10, termId = LeaderTerm.of(0, 1))
        )
        // A source watermark that walks backwards trips the watchers' monotonicity check, which is the
        // cheapest way to fault an apply without reaching for a mock.
        replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), srcMsgId = 5, termId = LeaderTerm.of(0, 1))
        )
        val unreached = replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 1)))

        val thrown = runCatching { logProc.awaitReplicaMsg(unreached.msgId) }.exceptionOrNull()

        assertTrue(
            thrown is IllegalStateException,
            "the await reports the tail's failure instead of waiting for a position it will never reach, got: $thrown"
        )

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a term equal to the highest seen is unfenced, one below it is not`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockBufferPool()
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = logProcessor(partitionStorage, partitionState, watchers, blockUploader, scope)

        logProc.termFence.admit(LeaderTerm.of(0, 9))

        // a transition checks its own claim once it has been read back, so the max *is* the claim
        assertDoesNotThrow { logProc.termFence.checkUnfenced(LeaderTerm.of(0, 9)) }
        assertThrows<Conflict> { logProc.termFence.checkUnfenced(LeaderTerm.of(0, 8)) }

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }
}
