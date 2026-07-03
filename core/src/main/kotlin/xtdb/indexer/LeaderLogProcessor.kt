package xtdb.indexer

import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.Metrics.withSpan
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult.Aborted
import xtdb.api.TransactionResult.Committed
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.arrow.VectorReader
import xtdb.arrow.asChannel
import xtdb.database.*
import xtdb.error.Anomaly
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.error.Interrupted
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.indexer.TxIndexer.TxResult
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.TrieKey
import xtdb.tx.deserializeUserMetadata
import xtdb.util.*
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import java.nio.ByteBuffer
import java.time.*

private val SKIPPED_EXN: Throwable = Fault("Transaction was skipped", "xtdb/skipped-tx")

private val LOG = LeaderLogProcessor::class.logger

class LeaderLogProcessor(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val dbStorage: DatabaseStorage,
    crashLogger: CrashLogger,
    private val dbState: DatabaseState,
    private val blockUploader: BlockUploader,
    private val watchers: Watchers,
    private val extSource: ExternalSource?,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    private val skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    partition: Int,
    afterReplicaMsgId: MessageId,
    private val instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration = Duration.ofMinutes(5),
    scope: CoroutineScope,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : LogProcessor.LeaderProcessor, TxIndexer {

    init {
        require((dbCatalog != null) == (dbState.name == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val dbName = dbState.name
    private val sourceLog = dbStorage.sourceLog
    private val bufferPool = dbStorage.bufferPool
    private val liveIndex = dbState.liveIndex

    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog

    private val allocator = allocator.newChildAllocator("leader-log-processor", 0, Long.MAX_VALUE)

    private val tracer = nodeBase.tracer?.takeIf { nodeBase.config.tracer.transactionTracing }

    private val sourceLogTxIndexer = SourceLogTxIndexer(this.allocator, nodeBase, dbState, crashLogger)

    // The GCs run under a SupervisorJob child of the leader scope, so one GC's failure cancels
    // neither its sibling nor the persister; cancelling the leader scope reaps them all.
    private val gcScope = CoroutineScope(scope.coroutineContext + SupervisorJob(scope.coroutineContext.job))

    internal val blockGc = nodeBase.config.garbageCollector.let { cfg ->
        BlockGarbageCollector(
            gcScope,
            bufferPool, blockCatalog,
            blocksToKeep = cfg.blocksToKeep,
            enabled = cfg.enabled,
            meterRegistry = nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
            dbName = dbName
        )
    }

    override var pendingBlock: PendingBlock? = null
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    private sealed interface PersisterTask {
        val onComplete: CompletableDeferred<Unit>
    }

    private sealed interface SourceLogTask : PersisterTask {
        // One task per poll batch; the persister resolves + imports the records in order.
        // onComplete is required by PersisterTask but unused here — processRecords fires and returns.
        data class Batch(val records: List<Log.Record<SourceMessage>>) : SourceLogTask {
            override val onComplete = CompletableDeferred<Unit>()
        }
    }

    private sealed interface ExtSourceTask : PersisterTask {
        class IndexTx(
            val externalSourceToken: ExternalSourceToken?,
            val systemTime: Instant?,
            val writer: suspend (OpenTx) -> TxResult,
        ) : ExtSourceTask {
            val result = CompletableDeferred<TxResult>()

            override val onComplete = CompletableDeferred<Unit>()
        }
    }

    private sealed interface GcTask : PersisterTask {
        data class TriesDeleted(val tableName: TableRef, val trieKeys: Set<TrieKey>) : GcTask {
            override val onComplete = CompletableDeferred<Unit>()
        }
    }

    // capacity 1: the poll thread can deposit one batch ahead and read the next while the persister
    // works, bounding lookahead to ~2 batches. Backpressure falls out of a full channel suspending the send.
    private val sourceLogCh =
        Channel<SourceLogTask>(capacity = 1, onUndeliveredElement = { it.onComplete.cancel() })
    // capacity 1 so a fire-and-forget `submitTx` caller can queue one tx ahead while the persister works the
    // current one (bounding lookahead to ~2). `executeTx` still blocks on the result regardless of capacity.
    private val extSourceCh =
        Channel<ExtSourceTask>(capacity = 1, onUndeliveredElement = { it.onComplete.cancel() })
    private val gcCh =
        Channel<GcTask>(onUndeliveredElement = { it.onComplete.cancel() })

    // The resolver (proc 1) owns the staging index: it resolves each tx, applies it to staging, and
    // hands the durable replica-log write to the replica writer (proc 2, sole owner of
    // `replicaProducer`) over `replicaCh`, awaiting confirmation on the `persistResultCh` back-edge.
    // Once durable, the resolver promotes the tx into the live index and notifies. Under this commit
    // the resolver awaits the back-edge per tx, so it stays fully serial and behaviour matches the
    // single-loop persister; the pipelining increment drops that per-tx await.
    private sealed interface ReplicaTask {
        // append a batch of resolved txs (source-log, ext-source, or attach/detach) to the replica log
        data class AppendTx(val batch: List<ReplicaMessage.ResolvedTx>) : ReplicaTask

        data class Forward(val message: ReplicaMessage, val srcMsgId: MessageId) : ReplicaTask

        data class FlushBlockFinish(
            val latestProcessedMsgId: MessageId, val externalSourceToken: ExternalSourceToken?
        ) : ReplicaTask

        data class TriesDeleted(val tableName: TableRef, val trieKeys: Set<TrieKey>) : ReplicaTask

        data class NotifyMsg(val msgId: MessageId) : ReplicaTask
    }

    // Seeded from the durable head so a new leader continues tx-ids from where the previous one left
    // off (rather than restarting from 0 and colliding with already-replicated tx-ids).
    private val staging = StagingIndex(liveIndex.latestCompletedTx)

    // resolver → replica writer: the durable-write work for a task.
    private val replicaCh = Channel<ReplicaTask>()
    // replica writer → resolver: the msgIds the writer appended for a task, one per appended message —
    // a batch of txs yields one per tx, in order — rendezvous. The resolver advances the apply cursor
    // `latestReplicaMsgId` per tx as it promotes, so a partial promote leaves the cursor at what
    // actually imported. At most one task is in flight. Failure propagates by cancellation, not by
    // closing this channel: the resolver and writer share a coroutineScope (see `termJob`), so when one
    // fails the other is cancelled and its parked `send`/`receive` throws `CancellationException`; the
    // failing loop is the one that calls `notifyError`, so it fires once.
    private val persistResultCh = Channel<List<MessageId>>()

    private suspend fun handleSourceLogBatch(records: List<Log.Record<SourceMessage>>) {
        for (record in records) {
            LOG.trace { "[$dbName] leader: message ${record.msgId} (${record.message::class.simpleName})" }
            handleSourceLogRecord(record)
        }
        // Drain the poll batch's tail so processRecords returns with the persister quiescent (#5741).
        drainBatch()
    }

    private suspend fun handleSourceLogRecord(record: Log.Record<SourceMessage>) {
        val msgId = record.msgId

        when (val msg = record.message) {
            is SourceMessage.Tx, is SourceMessage.LegacyTx -> {
                // source-log txs carry their own source position, so the replicated record and the
                // notify both stamp `msgId`. Accumulate into the batch and keep the writer fed — settle
                // a ready ack, then send if the writer's free; the tail drains in handleSourceLogBatch.
                val resolved = resolveTx(msgId, record, msg)
                staging.apply(resolved.resolvedTx.copy(srcMsgId = msgId), resolved.openTx, notifyMsgId = msgId)
                settleIfReady(finishBlockIfFull = true)
                if (!staging.inFlight) sendAccumulated()
            }

            is SourceMessage.FlushBlock -> {
                drainBatch() // flush accumulated txs first, so the flush follows them on the log
                val expectedBlockIdx = msg.expectedBlockIdx
                if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                    latestReplicaMsgId = sendAndSettle(ReplicaTask.FlushBlockFinish(msgId, watchers.externalSourceToken))
                } else {
                    // see #5680
                    latestReplicaMsgId = sendAndSettle(ReplicaTask.Forward(ReplicaMessage.NoOp(srcMsgId = msgId), msgId))
                }
            }

            is SourceMessage.AttachDatabase -> {
                drainBatch() // isolate the DDL tx: flush accumulated data txs first
                val txKey = TransactionKey(msgId, record.logTimestamp)
                val error = if (dbCatalog != null) {
                    try {
                        dbCatalog.attach(msg.dbName, msg.config)
                        null
                    } catch (e: Anomaly.Caller) {
                        LOG.debug(e) { "[$dbName] leader: attach database '${msg.dbName}' failed at $msgId" }
                        e
                    }
                } else null

                val openTx = openTx(txKey, null)
                val resolvedTx = try {
                    openTx.commitTx(error).copy(srcMsgId = msgId)
                        .let { if (error == null) it.copy(dbOp = DbOp.Attach(msg.dbName, msg.config)) else it }
                } catch (e: Throwable) { openTx.close(); throw e }

                // attach/detach notify with the tx-id and never finish a block (asymmetry preserved).
                persistResolvedTx(resolvedTx, openTx, notifyMsgId = resolvedTx.txId, finishBlockIfFull = false)
            }

            is SourceMessage.DetachDatabase -> {
                drainBatch() // isolate the DDL tx: flush accumulated data txs first
                val txKey = TransactionKey(msgId, record.logTimestamp)
                val error = if (dbCatalog != null) {
                    try {
                        dbCatalog.detach(msg.dbName)
                        null
                    } catch (e: Anomaly.Caller) {
                        LOG.debug(e) { "[$dbName] leader: detach database '${msg.dbName}' failed at $msgId" }
                        e
                    }
                } else null

                val openTx = openTx(txKey, null)
                val resolvedTx = try {
                    openTx.commitTx(error).copy(srcMsgId = msgId)
                        .let { if (error == null) it.copy(dbOp = DbOp.Detach(msg.dbName)) else it }
                } catch (e: Throwable) { openTx.close(); throw e }

                persistResolvedTx(resolvedTx, openTx, notifyMsgId = resolvedTx.txId, finishBlockIfFull = false)
            }

            is SourceMessage.TriesAdded -> {
                drainBatch() // flush accumulated txs first, so TriesAdded follows them on the log
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch) {
                    msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                        trieCatalog.addTries(TableRef.parse(tableName), tries, record.logTimestamp)
                    }
                }

                latestReplicaMsgId = sendAndSettle(
                    ReplicaTask.Forward(
                        TriesAdded(msg.storageVersion, msg.storageEpoch, msg.tries, sourceMsgId = msgId), msgId
                    )
                )
            }

            // TODO this one's going after 2.2
            is SourceMessage.BlockUploaded -> {
                drainBatch() // flush accumulated txs first, so this follows them on the log
                sendAndSettle(ReplicaTask.NotifyMsg(msgId))
            }
        }
    }

    // Hand the durable-write work to the replica writer and await its confirmation — keeps the resolver
    // and writer in lock-step (fully serial). If the writer fails, the shared coroutineScope cancels
    // the resolver, so this `receive` (or a later `send`) throws `CancellationException` and the loop
    // unwinds; the writer reports the root cause via `notifyError`.
    private suspend fun sendAndSettle(task: ReplicaTask): MessageId {
        replicaCh.send(task)
        // control tasks append a single message; its msgId is the cursor position.
        return persistResultCh.receive().last()
    }

    // Resolver: apply the resolved tx to staging, hand the durable write to the writer, then — once
    // it's durable — promote it into the live index and notify. The writer only appends to the replica
    // log; the import/notify/block-finish live here, downstream of the durable write, so dropping the
    // per-tx await later (pipelining) doesn't move them.
    // --- the resolver's send/settle pipeline over the staging index ---
    //
    // Resolved txs accumulate in the staging index; the resolver seals a batch and hands it to the
    // writer (`sendAccumulated`, fire-and-forget), then keeps resolving while the writer appends, and
    // on the writer's ack promotes the batch into the durable index (`settle`). At most one batch is in
    // flight. Because `processRecords` awaits the whole poll batch draining (`drainBatch`) before it
    // returns, the persister is quiescent whenever the poll thread is back in `poll()` — so a rebalance
    // transition's cancel-join finds nothing in flight (#5741); the resolver never runs ahead of a poll.

    // Seal the accumulated txs and hand the batch to the writer; its ack is settled later.
    private suspend fun sendAccumulated() {
        replicaCh.send(ReplicaTask.AppendTx(staging.seal()))
    }

    // Promote the in-flight batch into the live index, advancing the apply cursor and notifying per tx,
    // and cut a block if the batch filled one. Runs on the ack — the durable index is accurate here, so
    // the block-full check needs no stage-time row-counting. The cursor advances per tx (not once at the
    // batch end) so a partial promote leaves it at the last imported tx — see `promoteNext`.
    private suspend fun settle(replicaMsgIds: List<MessageId>, finishBlockIfFull: Boolean) {
        var last: StagingIndex.Staged? = null
        var i = 0
        while (true) {
            val staged = staging.promoteNext(liveIndex) ?: break
            // i-th tx ↔ i-th appended msgId (both in send order).
            latestReplicaMsgId = replicaMsgIds[i++]
            val tx = staged.resolvedTx
            val txKey = TransactionKey(tx.txId, tx.systemTime)
            val txResult = if (tx.committed) Committed(txKey) else Aborted(txKey, tx.error)
            watchers.notifyTx(txResult, staged.notifyMsgId, tx.externalSourceToken)
            last = staged
        }

        if (finishBlockIfFull && liveIndex.isFull()) last?.let { l ->
            latestReplicaMsgId =
                sendAndSettle(ReplicaTask.FlushBlockFinish(l.notifyMsgId, l.resolvedTx.externalSourceToken))
        }
    }

    // Await the in-flight batch's ack and settle it.
    private suspend fun settleInFlight(finishBlockIfFull: Boolean) =
        settle(persistResultCh.receive(), finishBlockIfFull)

    // Settle the in-flight batch if its ack has already arrived (non-blocking) — keeps the writer fed
    // while the resolver works through the rest of the poll batch.
    private suspend fun settleIfReady(finishBlockIfFull: Boolean) {
        if (staging.inFlight) persistResultCh.tryReceive().getOrNull()?.let { settle(it, finishBlockIfFull) }
    }

    // Flush and settle everything staged, blocking until the staging index is empty. Used before a
    // control message that must follow the accumulated txs on the replica log, and at the end of the
    // poll batch (so `processRecords` returns only once the persister is quiescent — see above).
    private suspend fun drainBatch() {
        while (staging.inFlight || staging.hasAccumulated) {
            if (staging.inFlight) settleInFlight(finishBlockIfFull = true)
            else sendAccumulated()
        }
    }

    // Resolve a single tx synchronously (batch-of-one): ext-source txs and attach/detach, which sit
    // outside the source-log poll-batch pipeline. Staging takes ownership of [openTx] at `apply` — it's
    // closed on promote (in `settle`) or on teardown (staging.close), never by the caller once handed over.
    private suspend fun persistResolvedTx(
        resolvedTx: ReplicaMessage.ResolvedTx, openTx: OpenTx, notifyMsgId: MessageId, finishBlockIfFull: Boolean,
    ) {
        staging.apply(resolvedTx, openTx, notifyMsgId)
        sendAccumulated()
        settleInFlight(finishBlockIfFull)
    }

    private suspend fun handleIndexTx(task: ExtSourceTask.IndexTx) {
        val txKey = TransactionKey(
            (staging.latestCompletedTx?.txId ?: -1) + 1,
            smoothSystemTime(task.systemTime ?: instantSource.instant())
        )

        var openTx = openTx(txKey, task.externalSourceToken)

        val writerResult: TxResult
        val resolvedTx: ReplicaMessage.ResolvedTx
        try {
            writerResult = task.writer(openTx)
            resolvedTx = when (writerResult) {
                is TxResult.Committed ->
                    openTx.commitTx(error = null, writerResult.userMetadata)

                is TxResult.Aborted -> {
                    txErrorCounter?.increment()
                    openTx.close()
                    // fresh tx for the abort row — the original openTx may hold partial writes
                    openTx = openTx(txKey, task.externalSourceToken)
                    openTx.commitTx(writerResult.error, writerResult.userMetadata)
                }
            }
        } catch (e: Throwable) {
            // failed before handing the openTx to staging — free it here (staging owns it after apply)
            openTx.close()
            throw e
        }

        // Ext-source txs carry no source-log position of their own and track progress via
        // `externalSourceToken`, so they don't advance the leader's `latestSourceMsgId` (driven by
        // the source log). We do stamp the current source-log watermark onto the replicated record
        // and the notify: without it a follower's `latestSourceMsgId` lags between block
        // boundaries, and on promotion it resumes the source log from a stale point and replays an
        // already-covered block boundary.
        val effectiveSrcMsgId = watchers.latestSourceMsgId
        persistResolvedTx(
            resolvedTx.copy(srcMsgId = effectiveSrcMsgId), openTx,
            notifyMsgId = effectiveSrcMsgId, finishBlockIfFull = true
        )
        task.result.complete(writerResult)
    }

    // Hand the task to the persister and return its completion handle. The caller decides whether to
    // await it: `executeTx`, GC and `processRecords` await (they need the work done before returning);
    // `submitTx` doesn't (fire-and-forget). Suspends only on the channel send (backpressure).
    private suspend fun enqueue(task: PersisterTask): Deferred<Unit> {
        when (task) {
            is SourceLogTask -> sourceLogCh.send(task)
            is ExtSourceTask -> extSourceCh.send(task)
            is GcTask -> gcCh.send(task)
        }
        return task.onComplete
    }

    internal val trieGc = nodeBase.config.garbageCollector.let { cfg ->
        // The replica-log append and the local catalog mutation are one atom — both run inside
        // a single Persister task. If they were split, this interleaving would corrupt
        // persistent state:
        //
        //   1. Trie GC submits `TriesDeleted(G)` at replica position N, then (separately)
        //      submits the catalog mutation.
        //   2. Between the two, another Persister task — say an ext-source `commit` whose
        //      `liveIndex.isFull()` — runs `finishBlock`, which uploads table-block files
        //      snapshotting the current catalog. The catalog still has G in it (Trie GC's
        //      mutation hasn't happened yet), so the table-block file at replica position
        //      M > N records "catalog includes G" — even though the replica log already has
        //      `TriesDeleted` for G at N.
        //   3. Trie GC's catalog mutation finally runs and removes G.
        //
        // The table-block file uploaded at (2) is now a persistent snapshot of state that
        // disagrees with the replica log it claims to be a snapshot of.
        val commitTriesDeleted: suspend (TableRef, Set<TrieKey>) -> Unit = { tableName, trieKeys ->
            enqueue(GcTask.TriesDeleted(tableName, trieKeys)).await()
        }

        TrieGarbageCollector(
            gcScope,
            bufferPool, dbState,
            commitTriesDeleted, cfg.blocksToKeep, cfg.garbageLifetime,
            cfg.enabled,
            nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
        )
    }

    private val txErrorCounter: Counter? = nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    // proc 1: the resolver. Serialises source-log / ext-source / GC, resolves each, and hands the
    // durable-write work to the writer. On a task failure it fails that task's awaiter and rethrows —
    // which cancels the writer through the enclosing coroutineScope (see `termJob`).
    private suspend fun resolverLoop() {
        while (true) {
            val task: PersisterTask = selectUnbiased {
                sourceLogCh.onReceive { it }
                extSourceCh.onReceive { it }
                gcCh.onReceive { it }
            }
            try {
                when (task) {
                    is SourceLogTask.Batch -> handleSourceLogBatch(task.records)
                    is ExtSourceTask.IndexTx -> handleIndexTx(task)
                    is GcTask.TriesDeleted ->
                        latestReplicaMsgId = sendAndSettle(ReplicaTask.TriesDeleted(task.tableName, task.trieKeys))
                }
                task.onComplete.complete(Unit)
            } catch (e: CancellationException) {
                task.onComplete.cancel(e)
                throw e
            } catch (e: InterruptedException) {
                task.onComplete.completeExceptionally(e)
                throw e
            } catch (e: Interrupted) {
                task.onComplete.completeExceptionally(e)
                throw e
            } catch (e: Throwable) {
                watchers.notifyError(e)
                task.onComplete.completeExceptionally(e)
                throw e
            }
        }
    }

    // proc 2: the replica-writer. Sole owner of `replicaProducer` — it only writes to the replica log;
    // the resolver does the staging promote / live-index import / notify downstream of the durable
    // write. Reports each task's applied replica position on `persistResultCh`. On its own failure it
    // notifies and rethrows — which cancels the resolver through the enclosing coroutineScope.
    private suspend fun writerLoop() {
        try {
            for (task in replicaCh) {
                // Append + apply each task, then report the replica position it lands at on the
                // back-edge. The writer never touches `latestReplicaMsgId`: the resolver advances that
                // apply cursor in ack-order (after promote for a tx, on the ack for a control message),
                // so the cursor stays a gap-free prefix even once the resolver runs ahead of it.
                val replicaMsgIds: List<MessageId> = when (task) {
                    is ReplicaTask.AppendTx -> appendBatchToReplica(task.batch)

                    is ReplicaTask.Forward -> {
                        val msgId = appendToReplica(task.message).msgId
                        watchers.notifyMsg(task.srcMsgId)
                        listOf(msgId)
                    }

                    is ReplicaTask.FlushBlockFinish -> {
                        val msgId = finishBlock(task.latestProcessedMsgId, task.externalSourceToken)
                        watchers.notifyMsg(task.latestProcessedMsgId)
                        listOf(msgId)
                    }

                    is ReplicaTask.TriesDeleted -> {
                        val msgId = appendToReplica(ReplicaMessage.TriesDeleted(task.tableName.schemaAndTable, task.trieKeys)).msgId
                        trieCatalog.deleteTries(task.tableName, task.trieKeys)
                        listOf(msgId)
                    }

                    is ReplicaTask.NotifyMsg -> {
                        watchers.notifyMsg(task.msgId)
                        // NotifyMsg moves only the source watermark, not the replica apply cursor;
                        // the resolver discards this ack value.
                        listOf(task.msgId)
                    }
                }
                persistResultCh.send(replicaMsgIds)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Interrupted) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
            throw e
        }
    }

    private val termJob: Job = scope.launch {
        // The external source is isolated under the supervisorScope so its crash doesn't kill the
        // persister. The persister's two halves — resolver and replica-writer — are lock-step and
        // useless apart, so they run in a plain coroutineScope and fail together: a crash in either
        // cancels the other. Neither closes the coordination channels (`replicaCh`/`persistResultCh`)
        // on the way out — cancellation reaps both halves — so only the source channels are closed here.
        supervisorScope {
            launch {
                var cause: Throwable? = null
                try {
                    coroutineScope {
                        launch { resolverLoop() }
                        launch { writerLoop() }
                    }
                } catch (e: CancellationException) {
                    // term cancellation: close the source channels without an error cause
                } catch (t: Throwable) {
                    cause = t
                } finally {
                    // A subsequent `enqueue` send then throws the cause rather than a bare
                    // ClosedSendChannelException; an awaiting caller (`executeTx`, GC, `processRecords`)
                    // sees it through its `await`. This is also the safety net for fire-and-forget
                    // `submitTx`, and for any caller's next send once the persister has exited.
                    sourceLogCh.close(cause)
                    extSourceCh.close(cause)
                    gcCh.close(cause)
                }
            }

            extSource?.let { source ->
                launch {
                    try {
                        source.onPartitionAssigned(partition, watchers.externalSourceToken, this@LeaderLogProcessor)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        watchers.notifyError(e)
                    }
                }
            }
        }
    }

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = staging.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    private fun openTx(txKey: TransactionKey, externalSourceToken: ExternalSourceToken?) =
        OpenTx(allocator, nodeBase, dbStorage, dbState, txKey, externalSourceToken, tracer)
            // resolution reads see durable ⊕ staged ⊕ own — the resolver is staging's sole accessor.
            .also { it.staging = staging }

    override suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult =
        ExtSourceTask.IndexTx(externalSourceToken, systemTime, writer)
            .also { enqueue(it).await() }
            .result.await()

    override suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ) {
        // Fire-and-forget: enqueue and return without awaiting the completion handle. The task's
        // `result`/`onComplete` are completed by the persister but go unawaited here — an unrecoverable
        // failure closes the channel with its cause, so the next `enqueue` (this or `executeTx`) throws it.
        enqueue(ExtSourceTask.IndexTx(externalSourceToken, systemTime, writer))
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            blockFlusher.flushedTxId = sourceLog.appendMessage(flushMessage).msgId
        }
    }

    // Append to the replica log and return the metadata. Does NOT advance `latestReplicaMsgId` — that
    // is the apply cursor, advanced by the caller once the message is applied locally (the writer for
    // control / block-boundary messages, the resolver after promote for txs).
    private suspend fun appendToReplica(message: ReplicaMessage): Log.MessageMetadata =
        replicaProducer.withTx { tx -> tx.appendMessage(message) }.await()

    // Append a batch of resolved txs in one atomic transaction; returns the appended msgId per tx, in
    // order, so the resolver can advance the apply cursor per tx as it promotes them.
    private suspend fun appendBatchToReplica(batch: List<ReplicaMessage.ResolvedTx>): List<MessageId> =
        replicaProducer.withTx { tx -> batch.map { tx.appendMessage(it) } }.awaitAll().map { it.msgId }

    // Returns the replica msgId the block upload lands at; the writer hands it back to the resolver,
    // which advances the apply cursor in ack-order, like every other task.
    private suspend fun finishBlock(latestProcessedMsgId: MessageId, externalSourceToken: ExternalSourceToken?): MessageId {
        val boundaryMsg =
            BlockBoundary((blockCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, externalSourceToken)

        val boundaryMsgId = appendToReplica(boundaryMsg).msgId
        LOG.debug("[$dbName] block boundary b${boundaryMsg.blockIndex.asLexHex}: source=$latestProcessedMsgId, replica=$boundaryMsgId")

        pendingBlock = PendingBlock(boundaryMsgId, boundaryMsg)

        val uploadedMsgId = blockUploader.uploadBlock(replicaProducer, boundaryMsgId, boundaryMsg)
        pendingBlock = null

        // Safe to call from inside a Persister task: signal() just enqueues a cycle on the GC's
        // own coroutine; its `commitTriesDeleted` callback submits a fresh task that won't run
        // until this one returns.
        blockGc.signal()
        trieGc.signal()

        return uploadedMsgId
    }

    // A resolved source-log tx: its serialized form plus the OpenTx holding its (queryable) writes,
    // returned open — staging takes ownership of the OpenTx at `persistResolvedTx`.
    private class Resolved(val resolvedTx: ReplicaMessage.ResolvedTx, val openTx: OpenTx)

    private fun indexSourceLogTx(
        msgId: MessageId,
        msgTimestamp: Instant,
        txOps: VectorReader?,
        systemTime: Instant?,
        defaultTz: ZoneId?,
        user: String?,
        userMetadata: Any?,
    ): Resolved = tracer.withSpan(
        "xtdb.transaction",
        attributes = mapOf("operations.count" to (txOps?.valueCount ?: 0).toString()),
    ) {
        val userMetadataMap = userMetadata as? Map<*, *>
        val lcTx = staging.latestCompletedTx

        // If lc-tx's systemTime >= msgTimestamp, bump past it by 1µs; otherwise use msgTimestamp.
        // (`+1000ns` is `+1µs`.)
        val defaultSystemTime: Instant = lcTx?.systemTime?.let { lcSysTime ->
            if (lcSysTime >= msgTimestamp) lcSysTime.plusNanos(1_000) else null
        } ?: msgTimestamp

        // Specified system-time before lc-tx → invalid; abort with that error.
        // The aborted tx-key uses the *default* (smoothed) systemTime, not the rejected one,
        // so the tx-key still satisfies the monotonicity invariant.
        if (systemTime != null && lcTx != null && systemTime < lcTx.systemTime) {
            val txKey = TransactionKey(msgId, defaultSystemTime)
            val err = Incorrect(
                "specified system-time older than current tx",
                "invalid-system-time",
                mapOf(
                    "tx-key" to TransactionKey(msgId, systemTime),
                    "latest-completed-tx" to lcTx,
                ),
            )
            LOG.warn { "specified system-time '$systemTime' older than current tx '$lcTx'" }

            return@withSpan openTx(txKey, null).closeOnCatch { openTx ->
                txErrorCounter?.increment()
                Resolved(openTx.commitTx(err, userMetadataMap), openTx)
            }
        }

        val effectiveSystemTime = systemTime ?: defaultSystemTime
        val txKey = TransactionKey(msgId, effectiveSystemTime)

        // Handed to staging open (parked for resolution reads, closed on promote), so we close the
        // OpenTx here only on the failure paths: an indexing/commit error, or an abort — whose partial
        // writes are discarded, the abort row resolving on a fresh tx.
        val openTx = openTx(txKey, null)
        val result: TxResult = try {
            if (txOps == null)
                return@withSpan Resolved(openTx.commitTx(SKIPPED_EXN, userMetadataMap), openTx)

            val opts = SourceLogTxIndexer.TxOpts(
                txKey = txKey,
                currentTime = effectiveSystemTime,
                systemTime = effectiveSystemTime.asMicros,
                defaultTz = defaultTz,
                user = user,
            )

            sourceLogTxIndexer.ForTx(txOps, opts).indexTx(openTx)
        } catch (e: Throwable) {
            openTx.close()
            throw e
        }

        when (result) {
            is TxResult.Committed ->
                try {
                    Resolved(openTx.commitTx(null, userMetadataMap), openTx)
                } catch (e: Throwable) {
                    openTx.close()
                    throw e
                }

            is TxResult.Aborted -> {
                LOG.debug(result.error) { "aborted tx" }
                openTx.close()
                openTx(txKey, null).closeOnCatch { abortTx ->
                    txErrorCounter?.increment()
                    Resolved(abortTx.commitTx(result.error, userMetadataMap), abortTx)
                }
            }
        }
    }

    private fun resolveTx(
        msgId: MessageId, record: Log.Record<SourceMessage>, msg: SourceMessage
    ): Resolved {
        if (skipTxs.isNotEmpty() && skipTxs.contains(msgId)) {
            LOG.warn("[$dbName] Skipping transaction id $msgId - within XTDB_SKIP_TXS")

            val payload = when (msg) {
                is SourceMessage.Tx -> msg.encode()
                is SourceMessage.LegacyTx -> msg.payload
                else -> error("unexpected message type: ${msg::class}")
            }
            bufferPool.putObject("skipped-txs/${msgId.asLexDec}".asPath, ByteBuffer.wrap(payload))

            return indexSourceLogTx(msgId, record.logTimestamp, null, null, null, null, null)
        }

        return when (msg) {
            is SourceMessage.Tx -> {
                msg.txOps.asChannel.use { ch ->
                    Relation.StreamLoader(allocator, ch).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            loader.loadNextPage(rel)

                            val userMetadata = msg.userMetadata?.let { deserializeUserMetadata(allocator, it) }

                            indexSourceLogTx(
                                msgId, record.logTimestamp,
                                rel["tx-ops"],
                                msg.systemTime, msg.defaultTz, msg.user, userMetadata
                            )
                        }
                    }
                }
            }

            is SourceMessage.LegacyTx -> {
                msg.payload.asChannel.use { txOpsCh ->
                    Relation.StreamLoader(allocator, txOpsCh).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            loader.loadNextPage(rel)

                            val systemTime =
                                (rel["system-time"].getObject(0) as ZonedDateTime?)?.toInstant()

                            val defaultTz =
                                (rel["default-tz"].getObject(0) as String?).let { ZoneId.of(it) }

                            val userMetadata = rel.vectorForOrNull("user-metadata")?.getObject(0)
                            val user = rel.vectorForOrNull("user")?.getObject(0) as String?

                            indexSourceLogTx(
                                msgId, record.logTimestamp,
                                rel["tx-ops"].listElements,
                                systemTime, defaultTz, user, userMetadata
                            )
                        }
                    }
                }
            }

            else -> error("unexpected message type: ${msg::class}")
        }
    }

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        // Await the batch through the persister rather than firing and returning. The persister still
        // resolves + imports on its own thread (the heavy work is off the poll thread), but blocking
        // here until it's done keeps the shared consumer's poll loop and the persister in lock-step:
        // whenever the poll thread is back in `poll()` — where Kafka runs rebalance callbacks, which
        // run a leader/follower transition under `runBlocking` — the persister is quiescent, so a
        // concurrent DETACH/shutdown that must cancel-join the term doesn't wedge against in-flight
        // import work on a starved dispatcher (#5741).
        if (records.isNotEmpty()) enqueue(SourceLogTask.Batch(records)).await()
    }

    override fun close() {
        extSource?.close()
        replicaProducer.close()
        staging.close() // standing state the resolver owned; freed here, before the allocator
        allocator.close() // last: Arrow won't close it while a child buffer is live
    }
}
