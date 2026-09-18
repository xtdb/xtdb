package xtdb.indexer

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.DatabaseName
import xtdb.api.TableRef
import xtdb.api.TransactionResult
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.table.fromSchemaAndTable
import xtdb.trie.Trie
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.TxIndexer
import xtdb.indexer.LogProcessor.LogsDriver

internal class LeaderLogProcessorTest : LeaderTermTest() {

    @Test
    fun `an attach is applied when its record is read back, not when it resolves`() = runTest(timeout = 5.seconds) {
        val append = GatedAppend()
        val dbCatalog = RecordingDbCatalog()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            watchers = watchers,
            dbCatalog = dbCatalog,
            wrapDriver = append::wrap,
        )

        backgroundScope.launch {
            lp.srcLogProc.processRecords(
                listOf(
                    Log.Record(
                        0, 0, Instant.now(),
                        SourceMessage.AttachDatabase("new_db", Database.Config())
                    )
                )
            )
        }

        append.started.await()
        testScheduler.advanceUntilIdle()

        assertEquals(
            emptyList<DatabaseName>(), dbCatalog.attached,
            "resolved but not yet durable — a term that is superseded here must not have attached anything"
        )

        append.open()
        watchers.awaitTx(0)

        assertEquals(listOf("new_db"), dbCatalog.attached, "consume-back is what attaches it")
    }

    @Test
    fun `an interrupt on the append path leaves the database queryable`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val (proc, appender) = unstartedTerm(watchers, driver = { inner ->
            object : LogsDriver by inner {
                // LocalStorage converts a ClosedByInterruptException into this on both its write paths
                override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> =
                    throw InterruptedException("interrupted writing to storage")
            }
        })

        appender.append(ControlItem(ReplicaMessage.NoOp(termId = 1)))

        assertThrows<InterruptedException> { proc.runTerm(Channel(), afterSourceMessageId = -1) }

        assertNull(
            watchers.exception,
            "an interrupt ends the term without failing the database"
        )
    }

    @Test
    fun `an interrupt in the external source leaves the database queryable`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val extSource = mockk<ExternalSource>(relaxed = true) {
            coEvery { onPartitionAssigned(any(), any(), any()) } throws InterruptedException("interrupted")
        }

        val proc = unstartedTerm(watchers, extSource = extSource).proc

        // `run` parks once the adapter is done, so the interrupt's handling is observed by cancelling it.
        val job = backgroundScope.launch { proc.extSrcProc!!.run() }
        testScheduler.advanceUntilIdle()
        job.cancelAndJoin()

        assertNull(
            watchers.exception,
            "an interrupt ends the term without failing the database"
        )
    }

    @Test
    fun `an ext-source tx applied from the record alone does not advance the source watermark`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val proc = unstartedTerm(watchers, extSource = mockk(relaxed = true)).proc

        // Not in the resolver's queue, so it is re-materialised from the record — the path a promotion's
        // replay takes for every record the follower buffered.
        //
        // srcMsgId is null only on a pre-#5586 record, and a CDC tx's txId is a per-database counter
        // rather than a source-log offset — so there is nothing to recover the position from, and
        // standing still is the only value that keeps Watchers' srcMsgId non-decreasing against the
        // next BlockBoundary's, which carries the leader's genuine source-log position.
        proc.applyReplicaMessage(
            Log.Record(
                0, 0, Instant.now(),
                ReplicaMessage.ResolvedTx(0, Instant.now(), true, null, emptyMap(), srcMsgId = null, termId = 1)
            )
        )

        assertEquals(-1L, watchers.latestSourceMsgId)
    }

    private fun record(msgId: Long, msg: ReplicaMessage) = Log.Record(0, msgId, Instant.now(), msg)

    private fun sourceRecord(msgId: Long, msg: SourceMessage) = Log.Record(0, msgId, Instant.now(), msg)

    private fun txRecord(msgId: Long) =
        sourceRecord(msgId, SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))

    /** A committing external-source tx, optionally parking in [writer] while the leader's persister holds it. */
    private suspend fun LeaderLogProcessor.commitTx(writer: suspend () -> Unit = {}) =
        extSrcProc!!.executeTx(null) { writer(); TxIndexer.TxResult.Committed() }

    private fun uploaded(blockIdx: Long, latestProcessedMsgId: Long) =
        ReplicaMessage.BlockUploaded(
            Storage.VERSION, 0, blockIdx, latestProcessedMsgId, emptyList(), termId = 1
        )

    @Test
    fun `a block is held from its boundary until its own upload reads back`() = runTest(timeout = 5.seconds) {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val (proc, _, partitionState) = unstartedTerm(watchers)

        proc.applyReplicaMessage(record(0, ReplicaMessage.BlockBoundary(0, 5, termId = 1)))

        assertNotNull(
            proc.pendingBlock,
            "the block file has landed and the upload is appended, but this node has not adopted it"
        )
        assertNull(
            partitionState.tableCatalog.currentBlockIndex,
            "so the catalog has not moved past the block, which is what keeps it re-producible"
        )

        proc.applyReplicaMessage(record(1, uploaded(blockIdx = 0, latestProcessedMsgId = 5)))

        assertNull(proc.pendingBlock)
        assertEquals(0L, partitionState.tableCatalog.currentBlockIndex)
    }

    @Test
    fun `a record arriving behind an open block applies once the block closes`() = runTest(timeout = 5.seconds) {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val (proc, _, _) = unstartedTerm(watchers)

        proc.applyReplicaMessage(record(0, ReplicaMessage.BlockBoundary(0, 5, termId = 1)))

        proc.applyReplicaMessage(
            record(
                1,
                ReplicaMessage.ResolvedTx(7, Instant.now(), true, null, emptyMap(), srcMsgId = 6, termId = 1)
            )
        )

        assertEquals(
            -1L, watchers.latestTxId,
            "held: its rows belong to the block opening behind this one, and the live index is still on the one already snapshotted"
        )

        proc.applyReplicaMessage(record(2, uploaded(blockIdx = 0, latestProcessedMsgId = 5)))

        assertEquals(7L, watchers.latestTxId, "and applied on the drain, behind the close")
    }

    @Test
    fun `tries reach the catalog when their own record reads back`() = runTest(timeout = 5.seconds) {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val (proc, _, partitionState) = unstartedTerm(watchers)

        // the catalog silently drops a trie whose key it can't parse, so this has to be a real one
        val trieKey = Trie.l0Key(0).toString()
        val tries = listOf(
            TrieDetails.newBuilder()
                .setTableName("public/foo")
                .setTrieKey(trieKey)
                .setDataFileSize(100)
                .setTrieMetadata(trieMetadata {})
                .build()
        )

        proc.applyReplicaMessage(
            record(0, ReplicaMessage.TriesAdded(Storage.VERSION, 0, tries, sourceMsgId = 3, termId = 1))
        )

        assertEquals(
            listOf(trieKey), partitionState.trieCatalog.listAllTrieKeys(fromSchemaAndTable("public/foo"))
        )
        assertEquals(3L, watchers.latestSourceMsgId)
    }

    // Inert, because these two are about when the commit returns rather than about what it removes: a
    // `deleteTries` for a shard the catalog doesn't hold trips its own spec assertion.
    private fun TestScope.gcTerm(append: GatedAppend, termJob: Job = SupervisorJob(backgroundScope.coroutineContext.job)) =
        leaderProc(
            StandardTestDispatcher(testScheduler), trieCatalog = mockk(relaxed = true),
            wrapDriver = append::wrap, termJob = termJob,
        )

    private suspend fun LeaderLogProcessor.commitTriesDeleted() =
        gc.commitTriesDeleted(TableRef("public", "foo"), setOf("l01-rc-b00"))

    @Test
    fun `a GC commit returns only once its own record reads back`() = runTest(timeout = 5.seconds) {
        val append = GatedAppend()
        val lp = gcTerm(append)

        val commit = backgroundScope.async { lp.commitTriesDeleted() }

        append.started.await()
        testScheduler.advanceUntilIdle()

        assertFalse(
            commit.isCompleted,
            "the GC has already deleted the files, so it must not go on against a catalog that still lists them"
        )

        // a hang here fires runTest's timeout — that is the assertion
        append.open()
        commit.await()
    }

    @Test
    fun `a slow append does not stall resolution`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val append = GatedAppend()

        // Skipped txs each stage a real (aborted) row without needing a valid tx-ops payload.
        val n = 5L
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            skipTxs = (0 until n).toSet(),
            wrapDriver = append::wrap,
        )

        val records = (0 until n).map { txRecord(it) }

        // Resolution is decoupled from the append pump: the whole batch resolves and processRecords returns
        // even though the append is still stalled on the gate — reaching the assertions below is the proof.
        lp.srcLogProc.processRecords(records)
        append.started.await()
        assertFalse(append.isOpen, "sanity: nothing opened the append gate")

        // Once the append drains, every tx reaches the replica log — in send order.
        append.open()
        watchers.awaitTx(n - 1)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()
        assertEquals((0 until n).toList(), resolvedTxs.map { it.txId }, "all $n txs land, in send order")
    }

    @Test
    fun `executeTx returns only once its tx is durable`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val append = GatedAppend()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapDriver = append::wrap,
        )

        // launch the executeTx so we can observe its completion state without blocking the test
        val txJob = backgroundScope.async { lp.commitTx() }

        append.started.await()

        assertFalse(txJob.isCompleted, "executeTx must not return before the replica-log append settles")

        append.open()
        val result = txJob.await()

        assertTrue(result is TransactionResult.Committed, "executeTx returns Committed once durable")
    }

    /**
     * The exception [body] fails with, awaited rather than thrown, and named [what] in the failure.
     *
     * A caller parked in one of these tests has to be observed from outside, because an `async` that fails
     * propagates into the non-supervisor [TestScope.backgroundScope] and fails the test before the assertion
     * runs. A body that returns fails this deferred instead, so a caller freed wrongly shows up as that
     * assertion rather than as a passing test.
     */
    private fun TestScope.failureOf(what: String, body: suspend () -> Unit) =
        CompletableDeferred<Throwable>().also { outcome ->
            backgroundScope.launch {
                try {
                    body()
                    outcome.completeExceptionally(AssertionError("$what returned rather than failing"))
                } catch (e: Throwable) {
                    outcome.complete(e)
                    if (e is CancellationException) throw e
                }
            }
        }

    @Test
    fun `closing the leader term fails an awaiting GC commit rather than hanging`() = runTest(timeout = 5.seconds) {
        // Never opened, so the record never reads back and the commit is past the channel the exit drains:
        // only the in-flight sweep can free it.
        val append = GatedAppend()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = gcTerm(append, termJob)

        val commit = backgroundScope.async { lp.commitTriesDeleted() }

        append.started.await()
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        assertTrue(runCatching { commit.await() }.isFailure, "the in-flight commit must fail, not hang")
    }

    @Test
    fun `closing the leader term fails an awaiting executeTx rather than hanging`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val append = GatedAppend()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapDriver = append::wrap, termJob = termJob,
        )

        val thrown = failureOf("executeTx") { lp.commitTx() }

        append.started.await()
        termJob.cancelAndJoin()

        thrown.await()
    }

    @Test
    fun `closing the leader term fails a buffered, never-received executeTx`() = runTest(timeout = 5.seconds) {
        val writerEntered = CompletableDeferred<Unit>()
        val writerGate = CompletableDeferred<Unit>()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), termJob = termJob)

        // t1 parks the persister inside its writer, so t2's task sits buffered in the channel —
        // never received, so never staged: only the exit drain can unblock its caller.
        val t1 = backgroundScope.async { lp.commitTx { writerEntered.complete(Unit); writerGate.await() } }
        writerEntered.await()
        val t2 = backgroundScope.async { lp.commitTx() }
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        // t1 fails via the pre-stage catch (cancelled mid-writer); t2 via the buffered-task drain.
        // A hang on either fires runTest's timeout.
        assertTrue(runCatching { t1.await() }.isFailure, "the in-writer executeTx must fail, not hang")
        assertTrue(runCatching { t2.await() }.isFailure, "the buffered executeTx must fail, not hang")
    }

    @Test
    fun `closing the leader term fails a buffered, never-received source-log batch`() = runTest(timeout = 5.seconds) {
        val writerEntered = CompletableDeferred<Unit>()
        val writerGate = CompletableDeferred<Unit>()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), watchers = watchers, termJob = termJob)

        // Park the persister inside an ext-source writer, so the source batch below lands in sourceLogCh's
        // buffer and is never received.
        backgroundScope.launch {
            runCatching { lp.commitTx { writerEntered.complete(Unit); writerGate.await() } }
        }
        writerEntered.await()

        // processRecords stands in for the transport's poll thread: it awaits the batch's completion. If the
        // term dies without failing the buffered batch, this await never returns — the poll thread wedges,
        // the transport's unregister is never serviced, and DatabaseCatalog.close blows its bound (#5711).
        val thrown = failureOf("processRecords") { lp.srcLogProc.processRecords(listOf(txRecord(0))) }
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        // It has to fail as CANCELLATION. The transport treats anything else as a poll-loop failure and
        // fails the term job into the Database scope's handler, which poisons the watchers — so a
        // benign teardown would present as a terminal query failure. See SourceBatch.abandon.
        val e = thrown.await()
        assertTrue(e is CancellationException, "the poll thread must see cancellation, got: $e")
        assertNull(watchers.exception, "a benign term close must not poison the watchers")
    }

    @Test
    fun `resigning cancels a staged executeTx rather than surfacing the supersession`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Never opened, so the tx below is still awaiting durability when the term ends.
        val append = GatedAppend()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            leaderTerm = 1, wrapDriver = append::wrap, termJob = termJob,
        )

        val thrown = failureOf("executeTx") { lp.commitTx() }
        append.started.await()

        termJob.supersede()

        // The ext source's poll thread awaits this, and anything but a cancellation reaching it unwinds
        // into the Database scope's handler → notifyError. See SourceBatch.abandon.
        val e = thrown.await()
        assertTrue(e is CancellationException, "the staged executeTx is cancelled, got: $e")

        assertNull(watchers.exception, "a clean resignation must not poison the watchers")
    }

    @Test
    fun `resigning cancels in-flight source batches rather than surfacing the supersession`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Never opened. The BlockBoundary's append hangs here, so the cut never reads back and resolution
        // stays paused — which is what makes this deterministic: batch #1 parks as `pausedBatch` and batch #2
        // stays buffered in the driver's source-batch pipe, so the term resigns with both in flight.
        val append = GatedAppend()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            leaderTerm = 1, wrapDriver = append::wrap, termJob = termJob,
        )

        // Two batches, each standing in for the transport's poll thread awaiting `processRecords`.
        fun pollThread(msgId: Long) = failureOf("processRecords") {
            lp.srcLogProc.processRecords(listOf(sourceRecord(msgId, SourceMessage.FlushBlock(-1))))
        }

        val paused = pollThread(0)          // cuts the block, then parks mid-batch
        append.started.await()              // the boundary hit the gated append ⇒ we are paused
        val buffered = pollThread(1)        // sent while paused ⇒ buffered, received by nobody
        testScheduler.advanceUntilIdle()

        termJob.supersede()

        // See SourceBatch.abandon for why anything but a cancellation here reaches the watchers.
        for ((name, handle) in listOf("paused" to paused, "buffered" to buffered))
            assertTrue(
                handle.await() is CancellationException,
                "the $name batch must fail as cancellation, got: ${handle.await()}"
            )

        assertNull(watchers.exception, "a resignation must not poison the watchers")
    }

}
