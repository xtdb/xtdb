package xtdb.indexer

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
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
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        val dbCatalog = RecordingDbCatalog()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            watchers = watchers,
            dbCatalog = dbCatalog,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
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

        appendStarted.await()
        testScheduler.advanceUntilIdle()

        assertEquals(
            emptyList<DatabaseName>(), dbCatalog.attached,
            "resolved but not yet durable — a term that is superseded here must not have attached anything"
        )

        gate.complete(Unit)
        watchers.awaitTx(0)

        assertEquals(listOf("new_db"), dbCatalog.attached, "consume-back is what attaches it")
    }

    @Test
    fun `an interrupt on the append path leaves the database queryable`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val (proc, appender) = unstartedTerm(watchers, driver = { inner ->
            object : LogsDriver by inner {
                // LocalStorage converts a ClosedByInterruptException into this on both its write paths
                override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
                    throw InterruptedException("interrupted writing to storage")
            }
        })

        appender.append(ControlItem(ReplicaMessage.NoOp(termId = 1)))

        // returns once the pump's failure has ended the term
        proc.runTerm(Channel())

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
        proc.extSrcProc!!.run()

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
    private fun TestScope.gcTerm(gate: CompletableDeferred<Unit>, appendStarted: CompletableDeferred<Unit>, termJob: Job) =
        leaderProc(
            StandardTestDispatcher(testScheduler), trieCatalog = mockk(relaxed = true),
            wrapDriver = { gatedDriver(it, gate, appendStarted) }, termJob = termJob,
        )

    @Test
    fun `a GC commit returns only once its own record reads back`() = runTest(timeout = 5.seconds) {
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        val lp = gcTerm(gate, appendStarted, SupervisorJob(backgroundScope.coroutineContext.job))

        val commit = backgroundScope.async { lp.gc.commitTriesDeleted(TableRef("public", "foo"), setOf("l01-rc-b00")) }

        appendStarted.await()
        testScheduler.advanceUntilIdle()

        assertFalse(
            commit.isCompleted,
            "the GC has already deleted the files, so it must not go on against a catalog that still lists them"
        )

        // a hang here fires runTest's timeout — that is the assertion
        gate.complete(Unit)
        commit.await()
    }

    @Test
    fun `closing the leader term fails an awaiting GC commit rather than hanging`() = runTest(timeout = 5.seconds) {
        // Never opened, so the record never reads back and the commit is past the channel the exit drains:
        // only the in-flight sweep can free it.
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = gcTerm(gate, appendStarted, termJob)

        val commit = backgroundScope.async { lp.gc.commitTriesDeleted(TableRef("public", "foo"), setOf("l01-rc-b00")) }

        appendStarted.await()
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        assertTrue(runCatching { commit.await() }.isFailure, "the in-flight commit must fail, not hang")
    }

    @Test
    fun `a slow append does not stall resolution`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        // Skipped txs each stage a real (aborted) row without needing a valid tx-ops payload.
        val n = 5L
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            skipTxs = (0 until n).toSet(),
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        val now = Instant.now()
        val records = (0 until n).map {
            Log.Record(0, it, now.plusMillis(it), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
        }

        // Resolution is decoupled from the append pump: the whole batch resolves and processRecords returns
        // even though the append is still stalled on the gate — reaching the assertions below is the proof.
        lp.srcLogProc.processRecords(records)
        appendStarted.await()
        assertFalse(gate.isCompleted, "sanity: nothing opened the append gate")

        // Once the append drains, every tx reaches the replica log — in send order.
        gate.complete(Unit)
        watchers.awaitTx(n - 1)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()
        assertEquals((0 until n).toList(), resolvedTxs.map { it.txId }, "all $n txs land, in send order")
    }

    @Test
    fun `executeTx returns only once its tx is durable`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // launch the executeTx so we can observe its completion state without blocking the test
        val txJob = backgroundScope.async { lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() } }

        appendStarted.await()

        assertFalse(txJob.isCompleted, "executeTx must not return before the replica-log append settles")

        gate.complete(Unit)
        val result = txJob.await()

        assertTrue(result is TransactionResult.Committed, "executeTx returns Committed once durable")
    }

    @Test
    fun `closing the leader term fails an awaiting executeTx rather than hanging`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Gate that is never opened — the append will stall indefinitely unless the term is cancelled.
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
            termJob = termJob,
        )

        // Capture the executeTx failure; the runTest timeout guards against a hang if it never completes.
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() }
            } catch (e: CancellationException) {
                thrown.complete(e)
                throw e
            } catch (e: Throwable) {
                thrown.complete(e)
            }
        }

        appendStarted.await()

        // Cancel the leader term — the gate will never open, so without term-close propagation
        // executeTx would hang until the runTest timeout.
        termJob.cancelAndJoin()

        // If executeTx hangs, thrown never completes and runTest's timeout fires — that's the hang guard.
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
        val t1 = backgroundScope.async {
            lp.extSrcProc!!.executeTx(null) { writerEntered.complete(Unit); writerGate.await(); TxIndexer.TxResult.Committed() }
        }
        writerEntered.await()
        val t2 = backgroundScope.async { lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() } }
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
            runCatching {
                lp.extSrcProc!!.executeTx(null) { writerEntered.complete(Unit); writerGate.await(); TxIndexer.TxResult.Committed() }
            }
        }
        writerEntered.await()

        // processRecords stands in for the transport's poll thread: it awaits the batch's completion. If the
        // term dies without failing the buffered batch, this await never returns — the poll thread wedges,
        // the transport's unregister is never serviced, and DatabaseCatalog.close blows its bound (#5711).
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.srcLogProc.processRecords(listOf(
                    Log.Record(0, 0, Instant.now(), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
                ))
                thrown.complete(AssertionError("processRecords returned normally"))
            } catch (e: CancellationException) {
                thrown.complete(e); throw e
            } catch (e: Throwable) {
                thrown.complete(e)
            }
        }
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        // A hang here fires runTest's timeout — that's the regression guard.
        val e = thrown.await()
        assertFalse(
            e is AssertionError,
            "processRecords must fail when the term closes on a buffered batch, not return normally"
        )

        // ...and it must fail as CANCELLATION. The transport treats anything else as a poll-loop failure and
        // unwinds openGroupSubscription into the Database scope's handler, which poisons the watchers — so a
        // benign teardown would present as a terminal query failure. See SourceBatch.abandon.
        assertTrue(e is CancellationException, "the poll thread must see cancellation, got: $e")
        assertNull(watchers.exception, "a benign term close must not poison the watchers")
    }

    @Test
    fun `a higher-term record read back resigns the leader`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Gate our own append so this leader's tx never lands: the only record on the log will be the
        // higher-term one injected below, so consume-back reaches it. Term fencing on read-back is the sole
        // split-brain guard now the transactional producer is gone (#5817) — this exercises the resign path.
        val gate = CompletableDeferred<Unit>() // never opened
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            leaderTerm = 1,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // An executeTx staged and awaiting durability — the resignation must fail it, not hang it. Capture
        // its failure via a launch + Deferred: a failing `async` would propagate to the (non-supervisor)
        // backgroundScope and fail the test, so we don't await it directly.
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                thrown.complete(e)
            }
        }
        appendStarted.await()

        // A newer leader (term 2) has written to our replica log. Injected straight onto the underlying log
        // (past the gate), so consume-back reads it while our own term-1 append is still stalled.
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2))

        // term 2 > our term 1 → we resign; the term tears down and fails everything staged.
        val e = thrown.await()
        assertTrue(
            generateSequence(e) { it.cause }.any { it.message?.contains("superseded") == true },
            "the awaiting executeTx surfaces the supersession, got: $e"
        )

        // A clean resignation is expected, not a query fault: the watchers must not be poisoned — the
        // transport re-follows on the next rebalance.
        assertNull(watchers.exception, "a clean resignation must not poison the watchers")
    }

    @Test
    fun `resigning cancels in-flight source batches rather than surfacing the supersession`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Never opened. The BlockBoundary's append hangs here, so the cut never reads back and resolution
        // stays paused — which is what makes this deterministic: batch #1 parks as `pausedBatch` and batch #2
        // stays buffered in the driver's source-batch pipe, so the term resigns with both in flight.
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            leaderTerm = 1, wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // Two batches, each standing in for the transport's poll thread awaiting `processRecords`.
        fun pollThread(msgId: Long) = CompletableDeferred<Throwable>().also { outcome ->
            backgroundScope.launch {
                try {
                    lp.srcLogProc.processRecords(listOf(Log.Record(0, msgId, Instant.now(), SourceMessage.FlushBlock(-1))))
                    outcome.complete(AssertionError("processRecords returned normally"))
                } catch (e: Throwable) {
                    outcome.complete(e)
                }
            }
        }

        val paused = pollThread(0)          // cuts the block, then parks mid-batch
        appendStarted.await()               // the boundary hit the gated append ⇒ we are paused
        val buffered = pollThread(1)        // sent while paused ⇒ buffered, received by nobody
        testScheduler.advanceUntilIdle()

        // A newer leader writes at term 2 — injected past the gate, so consume-back reads it while paused
        // (replicaMsgs is the one select arm the pause leaves open) and the leader resigns.
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2))

        // Both must fail as CANCELLATION, not with the LeaderSupersededException. The poll thread awaits
        // these, and a non-cancellation escaping processRecords unwinds openGroupSubscription into the
        // Database scope's CoroutineExceptionHandler → notifyError, so a clean resignation would present to
        // queries as a terminal failure. See SourceBatch.abandon.
        for ((name, handle) in listOf("paused" to paused, "buffered" to buffered))
            assertTrue(
                handle.await() is CancellationException,
                "the $name batch must fail as cancellation, got: ${handle.await()}"
            )

        assertNull(watchers.exception, "a resignation must not poison the watchers")
    }

}
