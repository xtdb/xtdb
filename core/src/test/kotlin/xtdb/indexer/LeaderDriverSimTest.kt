package xtdb.indexer

import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
import xtdb.api.TableRef
import xtdb.api.TransactionResult
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.MemoryStorage
import xtdb.types.MessageId
import xtdb.util.closeAll
import java.time.InstantSource
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Two `LeaderLogProcessor`s, each believing it leads the same database, against one shared replica
 * log and one shared object store.
 *
 * This is what the [LeaderDriver] seam bought: a leader can be built and fed without a transport, so
 * a test can hold two of them live at once — something no rebalance-driven harness can produce,
 * because a rebalance demotes the old leader before promoting the new one.
 */
@Tag("property")
class LeaderDriverSimTest : SimulationTestBase() {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator
    private lateinit var bufferPool: MemoryStorage
    private lateinit var sourceLog: InMemoryLog<SourceMessage>
    private lateinit var replicaLog: InMemoryLog<ReplicaMessage>
    private var latestTerm = 0L

    private val leaders = mutableListOf<SimLeader>()

    /** Every *successful* block upload across both leaders, as (blockIndex, boundaryMsgId). */
    private val blockUploads = mutableListOf<Pair<Long, MessageId>>()

    private val docsTable = TableRef("public", "docs")

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
        bufferPool = MemoryStorage(allocator, epoch = 0)
        sourceLog = InMemoryLog(InstantSource.system(), 0)
        replicaLog = InMemoryLog(InstantSource.system(), 0)
        latestTerm = 0L
    }

    @AfterEach
    fun tearDown() {
        leaders.closeAll()
        bufferPool.close()
        allocator.close()
        nodeBase.close()
    }

    /**
     * One node's whole leader-side world: its own catalogs and live index (nothing is shared between
     * leaders except the replica log and the object store, which is the point).
     */
    private inner class SimLeader(
        name: String, rowsPerBlock: Long, scope: CoroutineScope,
        val termId: Long, afterReplicaMsgId: MessageId,
        wrapDriver: (LeaderDriver) -> LeaderDriver = { it },
    ) : AutoCloseable {

        private val indexerConfig = IndexerConfig(rowsPerBlock = rowsPerBlock)

        val blockCatalog = BlockCatalog(null)
        val tableCatalog = TableCatalog(bufferPool)
        val trieCatalog = createTrieCatalog()
        val liveIndex =
            LiveIndex.open(
                allocator, blockCatalog, tableCatalog, trieCatalog, indexerConfig, ioDispatcher = dispatcher
            )

        val partitionState = PartitionState(blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        private val applier = ReplicaApplier(
            allocator, bufferPool, partitionState, "test-db",
            mockk<Compactor.ForDatabase>(relaxed = true), watchers,
            dbCatalog = null, afterReplicaMsgId = afterReplicaMsgId, hasExternalSource = false,
        )

        val proc = LeaderLogProcessor(
            allocator, nodeBase, partitionStorage, CrashLogger(allocator, bufferPool, "sim-$name"),
            partitionState, "test-db",
            wrapDriver(
                RecordingDriver(
                    RealLeaderDriver(
                        partitionStorage, partitionState,
                        BlockUploader(
                            partitionStorage, partitionState, "xtdb", mockk<Compactor.ForDatabase>(relaxed = true),
                            null, null, scope, uploadDispatcher = dispatcher
                        )
                    )
                )
            ),
            mockk<Compactor.ForDatabase>(relaxed = true), watchers,
            extSource = null, skipTxs = emptySet(), dbCatalog = null,
            applier = applier,
            afterReplicaMsgId = afterReplicaMsgId,
            // Never left at the default: two leaders sharing term 0 would each read the other's records
            // back as its own, and the term is exactly what tells them apart.
            leaderTerm = termId,
            flushTimeout = indexerConfig.flushDuration,
            scope = scope, gcDispatcher = dispatcher,
        )

        /** Fire-and-forget: the returned handle completes only once the tx is durably replicated. */
        suspend fun submitRows(rows: List<UUID>): Deferred<TransactionResult> =
            proc.submitTx(null) { openTx ->
                val table = openTx.table(docsTable)
                for (id in rows) table.writePut(mapOf("_id" to id, "tx_id" to openTx.txKey.txId))
                TxResult.Committed()
            }

        /**
         * Submit and wait, yielding the acked tx-id or null if it never became durable.
         *
         * Both halves can throw once this leader has been fenced, and which one does is a race:
         * `submitTx` itself throws if the term's channels have already been closed with the fault
         * (the documented early-exit signal), otherwise the handle fails at settle. For these tests
         * the only distinction that matters is acked vs not.
         */
        suspend fun trySubmitRows(rows: List<UUID>): Long? =
            runCatching { submitRows(rows).await() }.getOrNull()
                .let { (it as? TransactionResult.Committed)?.txKey?.txId }

        override fun close() {
            proc.close()
            applier.close()
            partitionState.close()
        }
    }

    /** Records the boundary each block was actually uploaded for — see [assertBlockCutsAgree]. */
    private inner class RecordingDriver(private val inner: LeaderDriver) : LeaderDriver by inner {
        override suspend fun uploadBlock(boundaryMsgId: MessageId, termId: Long, boundary: BlockBoundary) =
            inner.uploadBlock(boundaryMsgId, termId, boundary)
                .also { blockUploads += boundary.blockIndex to boundaryMsgId }
    }

    /**
     * Open a leader the way a real transition does: claim the next term by appending a `NoOp` stamped with
     * it, and tail from that record.
     *
     * The claim has to reach the log. With the transactional producer gone, a superseded leader finds out
     * only by *reading back* a higher term (#5817) — so a leader that claimed silently would leave its
     * predecessor happily acking forever, which is not how `LogProcessor.runTransition` behaves.
     */
    private suspend fun openLeader(
        name: String, rowsPerBlock: Long, scope: CoroutineScope,
        wrapDriver: (LeaderDriver) -> LeaderDriver = { it },
    ): SimLeader {
        val termId = ++latestTerm
        val replayTarget = replicaLog.appendMessage(ReplicaMessage.NoOp(termId = termId)).msgId
        return SimLeader(name, rowsPerBlock, scope, termId, replayTarget, wrapDriver).also { leaders += it }
    }

    /**
     * The reader-side fence, as every follower and leader applies it: a record is ignored if a higher term
     * precedes it on the log. Term 0 is the legacy marker and is never fenced.
     */
    private fun fencedIn(): List<ReplicaMessage> {
        var maxTerm = 0L
        return replicaMessages().filter { msg ->
            val term = msg.termId
            if (term != 0L && term < maxTerm) false
            else { maxTerm = maxOf(maxTerm, term); true }
        }
    }

    // ---- observations of the shared replica log ----

    private fun replicaMessages(): List<ReplicaMessage> =
        replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1).map { it.message }.toList()

    private fun replicaTxIds() = replicaMessages().filterIsInstance<ReplicaMessage.ResolvedTx>().map { it.txId }

    /**
     * Two leaders uploading the same block index is *fine* — the upload is deterministic from the
     * boundary, so a redundant one is the same coordination-free idempotency the compactor relies on.
     * What must not happen is two leaders disagreeing about *which* cut block N is: a block is
     * identified by the replica-log position of its `BlockBoundary`, so every upload of index N must
     * name the same `boundaryMsgId`. Two uploads of N from different boundaries means two different
     * snapshots writing the same object-store paths.
     */
    private fun assertBlockCutsAgree() {
        for ((blockIndex, boundaryMsgIds) in blockUploads.groupBy({ it.first }, { it.second })) {
            assertEquals(
                1, boundaryMsgIds.distinct().size,
                "block b$blockIndex was uploaded for more than one boundary: " +
                        "${boundaryMsgIds.distinct()} (seed=$currentSeed)"
            )
        }
    }

    /**
     * The dangerous direction: a leader must never tell a caller its tx is durable unless the tx
     * actually reached the log. Falsely failing an appended tx is survivable (the caller retries);
     * falsely acking a lost one is not.
     */
    private fun assertNoPhantomAcks(acked: List<Long>) {
        val onLog = replicaTxIds().toSet()
        val phantom = acked.filterNot { it in onLog }
        assertTrue(
            phantom.isEmpty(),
            "acked txs that never reached the replica log: $phantom (seed=$currentSeed)"
        )
    }

    private suspend fun Deferred<TransactionResult>.ackedTxIdOrNull(): Long? =
        runCatching { (await() as TransactionResult.Committed).txKey.txId }.getOrNull()

    /**
     * Where the leaders' coroutines live: `backgroundScope` so `runTest` cancels them when the body
     * finishes without waiting for them, on the seeded dispatcher so the simulation stays
     * deterministic.
     *
     * A leader's term job runs its persister forever, so it must NOT be a child of anything the test
     * joins — otherwise the join waits on a coroutine that only the join's completion would cancel.
     */
    private val TestScope.leaderScope get() = CoroutineScope(backgroundScope.coroutineContext + dispatcher)

    private val randomRows get() = List(rand.nextInt(1, 4)) { UUID(rand.nextLong(), rand.nextLong()) }

    @RepeatableSimulationTest
    fun `a superseded leader is fenced, fails its term, and acks nothing it lost`() =
        runTest(timeout = 10.seconds) {
            val beforeCount = rand.nextInt(1, 5)
            val afterCount = rand.nextInt(1, 5)
            val scope = leaderScope

            val a = openLeader("A", rowsPerBlock = 10_000, scope = scope)

            // A leads alone: everything it submits here should land. Asserted, not assumed — without
            // this the fencing assertions below would pass just as happily against a leader that
            // never worked at all.
            val beforeAcked = List(beforeCount) { a.trySubmitRows(randomRows) }.filterNotNull()
            assertEquals(
                beforeCount, beforeAcked.size,
                "the sole leader must ack everything it submits before anyone supersedes it"
            )

            // B claims the next term, announcing it on the log — A is a zombie now, but only finds
            // out when its consume-back reaches that claim.
            val b = openLeader("B", rowsPerBlock = 10_000, scope = scope)

            val afterAcked = List(afterCount) { a.trySubmitRows(randomRows) }.filterNotNull()

            assertEquals(
                emptyList<Long>(), afterAcked,
                "a fenced leader must not ack anything it submitted after being superseded"
            )
            // A resignation, not a fault. Under the transactional producer this arrived as a
            // ProducerFenced surfacing through `notifyError`; the term fence makes it orderly, so the
            // watchers MUST stay clean — the transport re-follows on the next rebalance, and poisoning
            // the watchers would turn an expected handover into a terminal query failure.
            assertNull(
                a.watchers.exception,
                "a superseded leader must resign cleanly, not poison its watchers (seed=$currentSeed)"
            )

            // And it really did stop: everything A resolved after the claim is fenced out of the log's
            // applied subsequence, so no reader ever adopts it.
            assertTrue(
                fencedIn().filterIsInstance<ReplicaMessage.ResolvedTx>().none { it.termId == a.termId && it.txId !in beforeAcked },
                "nothing A wrote after being superseded may survive the reader-side fence (seed=$currentSeed)"
            )
            assertNoPhantomAcks(beforeAcked + afterAcked)

            // B is unaffected and can still write.
            val bAcked = b.trySubmitRows(randomRows)
            assertNotNull(bAcked, "the surviving leader must still be able to write")
            assertNoPhantomAcks(beforeAcked + afterAcked + listOfNotNull(bAcked))
        }

    /**
     * The one interleaving where two leaders really are concurrent: A is held inside `uploadBlock`,
     * having already appended its `BlockBoundary`, when B supersedes it.
     *
     * Note what changed with the transactional producer's removal. A fenced producer's commit was
     * rejected, so a superseded leader's `BlockUploaded` never reached the log at all. A plain append
     * always lands — so the record *is* on the log, stamped with A's now-stale term, and what keeps the
     * system consistent is that every reader discards it. The block files A wrote to the object store
     * land either way; that was true under the producer too, since the writes precede the commit.
     */
    @Test
    fun `a leader fenced mid-upload has its block ignored`() = runTest(timeout = 10.seconds) {
        val atUpload = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val scope = leaderScope

        val a = openLeader("A", rowsPerBlock = 2, scope = scope) { inner ->
            object : LeaderDriver by inner {
                override suspend fun uploadBlock(boundaryMsgId: MessageId, termId: Long, boundary: BlockBoundary): MessageId {
                    atUpload.complete(Unit)
                    release.await()
                    return inner.uploadBlock(boundaryMsgId, termId, boundary)
                }
            }
        }

        // Two rows fills the block, so settling this tx cuts a boundary and enters uploadBlock.
        val handle = a.submitRows(listOf(UUID(1, 1), UUID(2, 2)))

        atUpload.await()
        openLeader("B", rowsPerBlock = 2, scope = scope)
        release.complete(Unit)

        val acked = listOfNotNull(handle.ackedTxIdOrNull())

        // The scenario actually happened: A got its boundary onto the log (it wasn't yet fenced) and
        // was then inside uploadBlock when B superseded it. Without this the next assertion would
        // hold trivially for a leader that never cut a block at all.
        assertEquals(
            listOf(0L), replicaMessages().filterIsInstance<BlockBoundary>().map { it.blockIndex },
            "A should have announced block b0's boundary before being fenced"
        )

        // A's upload lands on the log, carrying its own (now stale) term...
        val uploaded = replicaMessages().filterIsInstance<ReplicaMessage.BlockUploaded>().singleOrNull()
        assertNotNull(uploaded, "A completed its upload, so its BlockUploaded is on the log")
        assertEquals(a.termId, uploaded!!.termId, "A must stamp its own term, not the one that superseded it")

        // ...and every reader drops it, because B's claim precedes it.
        assertTrue(
            fencedIn().none { it is ReplicaMessage.BlockUploaded },
            "a stale-term BlockUploaded must be fenced out by every reader (seed=$currentSeed)"
        )
        assertNoPhantomAcks(acked)
        assertBlockCutsAgree()
    }
}
