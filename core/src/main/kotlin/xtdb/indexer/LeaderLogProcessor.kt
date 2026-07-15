package xtdb.indexer

import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.Metrics.withSpan
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
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
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.table.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.TrieKey
import xtdb.tx.deserializeUserMetadata
import xtdb.util.*
import xtdb.util.StringUtil.asLexDec
import xtdb.util.StringUtil.asLexHex
import java.nio.ByteBuffer
import java.time.*
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken

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
) : LogProcessor.Processor<SourceMessage>, TxIndexer {

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

    var pendingBlock: PendingBlock? = null
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    private val maxStagedRows = nodeBase.config.indexer.rowsPerBlock

    // Resolver-owned staging area: resolved-but-not-yet-durable txs, seeded at the durable head. The
    // resolver is its sole accessor (no lock). Freed in close() (phase 2), once the resolver job is
    // joined so nothing live still touches it; see StagingIndex.
    private val stagingIndex = StagingIndex(liveIndex.latestCompletedTx)

    /**
     * A sealed batch: the txs bound for one replica-log producer transaction, in send order, together
     * with [append] — that producer transaction's background commit, yielding the per-tx replica-log
     * positions. One value on purpose: the txs and the append that carries them can't drift apart.
     */
    private inner class InFlightBatch(
        val txs: List<ResolvedTx>, private val append: Deferred<List<Pair<ResolvedTx, Log.MessageMetadata>>>
    ) : AutoCloseable {

        val isCompleted: Boolean get() = append.isCompleted

        // onJoin, not onAwait: join fires on failure too, letting settleAppend's own await rethrow
        // the fault inside the persister's try/catch, which routes it through notifyError.
        val onJoin get() = append.onJoin

        suspend fun settle() {
            val metadatas = append.await()

            for ((resolvedTx, metadata) in metadatas) {
                liveIndex.commitTx(resolvedTx.txKey, resolvedTx.allTables.associate { it.ref to it.relation })
                latestReplicaMsgId = metadata.msgId
                watchers.notifyTx(resolvedTx.txResult, resolvedTx.srcMsgId, resolvedTx.externalSourceToken)
            }

            // Watchers-derived rather than txs.last(): the promote loop above has just notified every
            // tx in send order, so latestSourceMsgId equals the last tx's, and the token null-coalesces
            // to the batch's last non-null — for a mixed source-log/ext-source batch, txs.last() could
            // be a source-log tx whose null token would drop the CDC resume point from the boundary.
            // Matches the FlushBlock path, which already cuts blocks from the watchers' view.
            if (liveIndex.isFull()) finishBlock(watchers.latestSourceMsgId, watchers.externalSourceToken)

            // Complete after the whole settle (promote loop + any block cut): signalling per-tx mid-loop
            // lets a caller race ahead of finishBlock, and a hard cancel then can orphan a BlockBoundary
            // with no BlockUploaded (the #5783 sim regression caught at 15/300).
            txs.forEach { it.pending?.complete(it.txResult) }
        }

        fun failPending(cause: Throwable) = txs.forEach { it.pending?.completeExceptionally(cause) }

        override fun close() {
            txs.closeAll()
        }
    }

    /**
     * The sealed-but-not-yet-promoted batch. At most one — the single fenced producer permits only one
     * open transaction at a time, and holding it as a nullable field makes that invariant structural
     * rather than checked at every append site.
     */
    private var inFlight: InFlightBatch? = null

    private val resolvedTxs get() = inFlight?.txs.orEmpty() + stagingIndex.resolvedTxs

    // Seal whatever has accumulated and launch its replica-log append — each tx serializing itself into
    // a replica message (ResolvedTx.toReplicaMessage), all appended in one fenced producer transaction —
    // in the background, so the resolver keeps resolving while the serialize + producer commit run.
    // At most one append is in flight, held on `inFlight`; the persister settles it (settleAppend) once
    // it completes. No-op if a batch is already in flight (the accumulating tail rides the next kick,
    // at settle) or there's nothing staged.
    private fun kickAppend(): InFlightBatch? =
        if (inFlight != null) null
        else stagingIndex.seal()?.closeAllOnCatch { txs ->
            InFlightBatch(
                txs,
                appendScope.async {
                    replicaProducer
                        .withTx { tx ->
                            txs.map { it to tx.appendMessage(it.toReplicaMessage()) }
                        }
                        .map { it.first to it.second.await() }
                }
            ).also { this.inFlight = it }
        }

    // The batch is freed here only once settle returns; on a settle fault — including a teardown
    // cancellation thrown out of its await while the append coroutine may still be serializing the
    // slices — it stays on `inFlight`, and close() frees it after cancelAndJoin has joined the append.
    private suspend fun settleAppend() {
        val batch = inFlight ?: return
        batch.settle()
        inFlight = null
        batch.close()
        kickAppend()
    }

    // Opportunistically settle a *completed* append between records — promoting it and kicking
    // the accumulated tail mid-batch — without ever suspending on one that's still going.
    private suspend fun trySettleAppend() {
        if (inFlight?.isCompleted == true) settleAppend()
    }

    // Fully drain: kick anything staged and settle appends until nothing is staged or in flight. The
    // boundary callers — control messages, attach/detach, ext-source txs, GC's direct appends, the
    // poll-boundary drain — rely on this post-condition: the replica log carries everything that
    // preceded them in resolution order, and the fenced producer is idle for their own append.
    private suspend fun drainStaging() {
        while (true) {
            kickAppend()
            if (inFlight == null) return
            settleAppend()
        }
    }


    // What the persister wakes up for: a task from one of the channels, or the in-flight append
    // completing (Settle). Settle is select-driven — with ext-source handlers no longer draining,
    // this arm is what promotes their batches even when no task is queued.
    private sealed interface PersisterWork
    private data object Settle : PersisterWork

    private sealed interface PersisterTask : PersisterWork {
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
            val result = CompletableDeferred<TransactionResult>()

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
        Channel<ExtSourceTask>(capacity = 1, onUndeliveredElement = { task ->
            task.onComplete.cancel()
            // Also cancel the per-tx durability handle: the task was never delivered to the persister,
            // so settle() will never complete it — this is the only path that can.
            if (task is ExtSourceTask.IndexTx) task.result.cancel()
        })

    private val gcCh =
        Channel<GcTask>(onUndeliveredElement = { it.onComplete.cancel() })

    private suspend fun handleSourceLogBatch(records: List<Log.Record<SourceMessage>>) {
        for (record in records) {
            LOG.trace { "[$dbName] leader: message ${record.msgId} (${record.message::class.simpleName})" }
            handleSourceLogRecord(record)

            trySettleAppend()
        }

        // Poll-boundary drain: flush any accumulated tail so the batch task returns with the resolver
        // quiescent — nothing in flight when the poll loop re-enters poll() and Kafka may run a
        // rebalance/transition under runBlocking (#5741).
        drainStaging()
    }

    private suspend fun handleSourceLogRecord(record: Log.Record<SourceMessage>) {
        val msgId = record.msgId
        val msg = record.message

        // Data txs accumulate into the staging batch and are committed together at the next drain. Every
        // other message is a boundary that must first drain the accumulated txs: a control message's
        // replica-log append has to land after the appends of the txs that preceded it in source order,
        // and a block cut needs those txs durable before the block is written.
        if (msg !is SourceMessage.Tx && msg !is SourceMessage.LegacyTx) drainStaging()

        when (msg) {
            is SourceMessage.Tx, is SourceMessage.LegacyTx -> {
                val (txResult, openTx) = resolveTx(msgId, record, msg)
                openTx.use {
                    stagingIndex.stage(it, msgId, txResult, dbOp = null, pending = null)
                }
                kickAppend()
            }

            is SourceMessage.FlushBlock -> {
                val expectedBlockIdx = msg.expectedBlockIdx
                if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                    finishBlock(msgId, watchers.externalSourceToken)
                } else {
                    // see #5680
                    appendToReplica(ReplicaMessage.NoOp(srcMsgId = msgId))
                }
                watchers.notifyMsg(msgId)
            }

            is SourceMessage.AttachDatabase -> {
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

                openTx(txKey, null).use { openTx ->
                    openTx.writeTxRow(error, null)
                    val txResult = if (error == null) Committed(txKey) else Aborted(txKey, error)
                    val dbOp = if (error == null) DbOp.Attach(msg.dbName, msg.config) else null
                    stagingIndex.stage(openTx, msgId, txResult, dbOp, pending = null)
                }
                drainStaging()
            }

            is SourceMessage.DetachDatabase -> {
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

                openTx(txKey, null).use { openTx ->
                    openTx.writeTxRow(error, null)
                    val txResult = if (error == null) Committed(txKey) else Aborted(txKey, error)
                    val dbOp = if (error == null) DbOp.Detach(msg.dbName) else null
                    stagingIndex.stage(openTx, msgId, txResult, dbOp, pending = null)
                }
                drainStaging()
            }

            is SourceMessage.TriesAdded -> {
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch) {
                    msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                        trieCatalog.addTries(fromSchemaAndTable(tableName), tries, record.logTimestamp)
                    }
                }

                appendToReplica(TriesAdded(msg.storageVersion, msg.storageEpoch, msg.tries, sourceMsgId = msgId))

                watchers.notifyMsg(msgId)
            }

            // TODO this one's going after 2.2
            is SourceMessage.BlockUploaded -> {
                watchers.notifyMsg(msgId)
            }
        }
    }

    private suspend fun handleIndexTx(task: ExtSourceTask.IndexTx) {
        val txKey = TransactionKey(
            (stagingIndex.latestCompletedTx?.txId ?: -1) + 1,
            smoothSystemTime(task.systemTime ?: instantSource.instant())
        )

        var openTx = openTx(txKey, task.externalSourceToken)

        @Suppress("ConvertTryFinallyToUseCall") // because openTx is a var
        try {
            try {
                val writerResult = task.writer(openTx)
                val txResult: TransactionResult = when (writerResult) {
                    is TxResult.Committed -> {
                        openTx.writeTxRow(null, writerResult.userMetadata)
                        Committed(txKey)
                    }

                    is TxResult.Aborted -> {
                        txErrorCounter?.increment()
                        openTx.close()
                        // fresh tx for the abort row — the original openTx may hold partial writes
                        openTx = openTx(txKey, task.externalSourceToken)
                        openTx.writeTxRow(writerResult.error, writerResult.userMetadata)
                        Aborted(txKey, writerResult.error)
                    }
                }

                // Ext-source txs carry no source-log position of their own and track progress via
                // `externalSourceToken`, so they don't advance `latestSourceMsgId` (driven by the source log).
                // We do stamp the current source-log watermark onto the replicated record: without it a
                // follower's `latestSourceMsgId` lags between block boundaries, and on promotion it resumes
                // the source log from a stale point and replays an already-covered block boundary.
                val effectiveSrcMsgId = watchers.latestSourceMsgId
                stagingIndex.stage(openTx, effectiveSrcMsgId, txResult, dbOp = null, pending = task.result)
            } catch (e: Throwable) {
                // Writer, writeTxRow, or stage threw before the tx was handed to the in-flight batch.
                // The persister finally won't see a staged entry for this tx, so complete the handle here —
                // the caller awaiting task.result would otherwise hang until the term closes.
                task.result.completeExceptionally(e)
                throw e
            }
            kickAppend()
            trySettleAppend()
            // Safety bound: never accumulate more than a block's worth of rows — a bursty source
            // must not grow staging without limit behind one slow commit (nor leave durability to
            // the ~5-min FlushBlock). Rows, not bytes: it's the dimension the block-sizing machinery
            // (isFull/rowsPerBlock) already manages, and the drain it triggers is ordinary
            // backpressure, not a failure.
            if (stagingIndex.rowCount > maxStagedRows) drainStaging()
        } finally {
            openTx.close()
        }
    }

    private suspend fun handleTriesDeleted(task: GcTask.TriesDeleted) {
        // Drain first: this appends TriesDeleted directly, and replica appends must stay in
        // resolver-processing order — appending ahead of earlier-staged txs would invert the log.
        drainStaging()
        appendToReplica(ReplicaMessage.TriesDeleted(task.tableName.schemaAndTable, task.trieKeys))
        trieCatalog.deleteTries(task.tableName, task.trieKeys)
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

    private val txErrorCounter: Counter? = nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    // The term handle: a supervisor child of the Database scope, owning the persister body (launched
    // last, in the `init` below) and the GCs. A term-internal failure surfaces via `notifyError`
    // rather than cancelling the source-log subscription — its sibling under the Database scope's own
    // SupervisorJob — and `cancelAndJoin` reaps the whole term. See dev/doc/coroutines.adoc.
    private val termJob = SupervisorJob(scope.coroutineContext.job)

    // The GCs run under a SupervisorJob child of `termJob`, so one GC's failure cancels neither its
    // sibling nor the persister; cancelling `termJob` reaps them all.
    private val gcScope = scope + SupervisorJob(termJob)

    // The in-flight replica-log append runs under its own SupervisorJob child of `termJob` for the same
    // isolation: a failed append must NOT cancel the persister out from under settleAppend — the failure
    // is held on the batch's Deferred and rethrown at settle, inside the task-handling try, so it
    // surfaces through notifyError — while `cancelAndJoin` on the term still reaps an append mid-flight
    // (the fenced producer transaction aborts, so nothing partial reaches the replica log).
    private val appendScope = scope + SupervisorJob(termJob)

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

    // Launched last so every field the body reaches — e.g. blockGc/trieGc via finishBlock — is
    // initialised before the first record. Runs under `termJob`, so `cancelAndJoin` reaps it.
    init {
        CoroutineScope(scope.coroutineContext + termJob).launch {
            launch {
                // Close the channels with the failure cause so a subsequent `enqueue` send throws it rather
                // than a bare ClosedSendChannelException. An awaiting caller (`executeTx`, GC, `processRecords`)
                // sees the cause through its `await`; this close-with-cause is the safety net for fire-and-forget
                // `submitTx`, and for any caller's next send once the persister loop has exited.
                var cause: Throwable? = null
                try {
                    while (true) {
                        // Every stage is followed by a kick and every settle re-kicks the accumulated tail,
                        // so staged txs always have an in-flight append ahead of them; a violation here
                        // means a wedge, not a race.
                        check(stagingIndex.isEmpty || inFlight != null) {
                            "staged txs with no in-flight append — nothing will kick them"
                        }

                        val work = selectUnbiased<PersisterWork> {
                            // onJoin, not onAwait: join fires on failure too, and settleAppend's own await
                            // rethrows the fault inside the try below, routing it through notifyError.
                            inFlight?.let { batch -> batch.onJoin { Settle } }
                            sourceLogCh.onReceive { it }
                            extSourceCh.onReceive { it }
                            gcCh.onReceive { it }
                        }

                        when (work) {
                            // Mirrors the task catches below: interrupts are shutdown signals, not
                            // ingestion faults — they mustn't poison the watchers on their way out.
                            Settle ->
                                try {
                                    settleAppend()
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

                            is PersisterTask -> {
                                val task = work
                                try {
                                    when (task) {
                                        is SourceLogTask.Batch -> handleSourceLogBatch(task.records)
                                        is ExtSourceTask.IndexTx -> handleIndexTx(task)
                                        is GcTask.TriesDeleted -> handleTriesDeleted(task)
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
                    }
                } catch (_: CancellationException) {
                    // term cancellation: close the channels without an error cause
                } catch (t: Throwable) {
                    cause = t
                } finally {
                    // Fail any ext-source tx that was staged or in-flight but will never settle:
                    // the persister is exiting and settle() will never run for them.
                    val pendingCause = cause ?: CancellationException("leader term closed")
                    inFlight?.failPending(pendingCause)
                    stagingIndex.failPending(pendingCause)

                    // A buffered-but-never-received task is invisible to both failPending (it was
                    // never staged) and onUndeliveredElement (close() doesn't visit buffered
                    // elements — only cancel() does), so a caller outside the term scope would
                    // await it forever.
                    while (true) {
                        val task = extSourceCh.tryReceive().getOrNull() ?: break
                        task.onComplete.completeExceptionally(pendingCause)
                        if (task is ExtSourceTask.IndexTx) task.result.completeExceptionally(pendingCause)
                    }

                    sourceLogCh.close(cause)
                    extSourceCh.close(cause)
                    gcCh.close(cause)
                }
            }

            extSource?.let { source ->
                supervisorScope {
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
    }

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = stagingIndex.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    private fun openTx(txKey: TransactionKey, externalSourceToken: ExternalSourceToken?) =
        OpenTx(allocator, nodeBase, dbStorage, dbState, txKey, externalSourceToken, tracer, resolvedTxs)

    override suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TransactionResult =
        submitTx(externalSourceToken, systemTime, writer).await()

    override suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): Deferred<TransactionResult> {
        val task = ExtSourceTask.IndexTx(externalSourceToken, systemTime, writer)
        // enqueue's send throws if the channel is closed (dead indexer) — that's the early-exit signal.
        // The returned handle is `task.result`, completed at settle once the tx is durably replicated; a
        // fire-and-forget caller may discard it, and an unrecoverable failure also closes the channel with
        // its cause, so the next `enqueue` throws it.
        enqueue(task)
        return task.result
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(blockCatalog, liveIndex)) {
            val flushMessage = SourceMessage.FlushBlock(blockCatalog.currentBlockIndex ?: -1)
            blockFlusher.flushedTxId = sourceLog.appendMessage(flushMessage).msgId
        }
    }

    private suspend fun appendToReplica(message: ReplicaMessage): Log.MessageMetadata =
        replicaProducer.withTx { tx -> tx.appendMessage(message) }.await()
            .also { latestReplicaMsgId = it.msgId }

    private suspend fun finishBlock(latestProcessedMsgId: MessageId, externalSourceToken: ExternalSourceToken?) {
        val boundaryMsg =
            BlockBoundary((blockCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, externalSourceToken)

        val boundaryMsgId = appendToReplica(boundaryMsg).msgId
        LOG.debug("[$dbName] block boundary b${boundaryMsg.blockIndex.asLexHex}: source=$latestProcessedMsgId, replica=$boundaryMsgId")

        pendingBlock = PendingBlock(boundaryMsgId, boundaryMsg)

        latestReplicaMsgId = blockUploader.uploadBlock(replicaProducer, boundaryMsgId, boundaryMsg)
        pendingBlock = null

        // Safe to call from inside a Persister task: signal() just enqueues a cycle on the GC's
        // own coroutine; its `commitTriesDeleted` callback submits a fresh task that won't run
        // until this one returns.
        blockGc.signal()
        trieGc.signal()
    }

    // A fresh tx committing a single skip / abort / invalid-system-time row, returned LIVE — the caller
    // stages it (which slices its writes) then closes it. On any failure the tx is closed here so nothing
    // leaks. `countError` mirrors the pre-staging behaviour: skipped txs don't count as errors.
    private fun commitStandaloneTx(
        txKey: TransactionKey, error: Throwable, userMetadata: Map<*, *>?, countError: Boolean,
    ): Pair<TransactionResult, OpenTx> {
        if (countError) txErrorCounter?.increment()
        val openTx = openTx(txKey, null)
        return try {
            openTx.writeTxRow(error, userMetadata)
            Aborted(txKey, error) to openTx
        } catch (e: Throwable) {
            openTx.close(); throw e
        }
    }

    // Resolves a source-log tx and returns its result alongside the LIVE OpenTx that produced it — the
    // resolver stages that OpenTx (taking independent slices of its writes) and closes it afterwards.
    // Any OpenTx not returned (an aborted tx's partial-write attempt, or one whose commit throws) is closed
    // here.
    private fun indexSourceLogTx(
        msgId: MessageId,
        msgTimestamp: Instant,
        txOps: VectorReader?,
        systemTime: Instant?,
        defaultTz: ZoneId?,
        user: String?,
        userMetadata: Any?,
    ): Pair<TransactionResult, OpenTx> = tracer.withSpan(
        "xtdb.transaction",
        attributes = mapOf("operations.count" to (txOps?.valueCount ?: 0).toString()),
    ) {
        val userMetadataMap = userMetadata as? Map<*, *>
        // The APPLIED head (staging), not the durable head: a tx must system-time-smooth against the
        // staged predecessors it resolves behind, which lead the durable index.
        val lcTx = stagingIndex.latestCompletedTx

        // If lc-tx's systemTime >= msgTimestamp, bump past it by 1µs; otherwise use msgTimestamp.
        // (`+1000ns` is `+1µs`.)
        val defaultSystemTime: Instant = lcTx?.systemTime?.let { lcSysTime ->
            if (lcSysTime >= msgTimestamp) lcSysTime.plusNanos(1_000) else null
        } ?: msgTimestamp

        // Specified system-time before lc-tx → invalid; abort with that error.
        // The aborted tx-key uses the *default* (smoothed) systemTime, not the rejected one,
        // so the tx-key still satisfies the monotonicity invariant.
        if (systemTime != null && lcTx != null && systemTime < lcTx.systemTime) {
            val err = Incorrect(
                "specified system-time older than current tx",
                "invalid-system-time",
                mapOf(
                    "tx-key" to TransactionKey(msgId, systemTime),
                    "latest-completed-tx" to lcTx,
                ),
            )
            LOG.warn { "specified system-time '$systemTime' older than current tx '$lcTx'" }

            return@withSpan commitStandaloneTx(
                TransactionKey(msgId, defaultSystemTime), err, userMetadataMap, countError = true
            )
        }

        val effectiveSystemTime = systemTime ?: defaultSystemTime
        val txKey = TransactionKey(msgId, effectiveSystemTime)

        if (txOps == null)
            return@withSpan commitStandaloneTx(txKey, SKIPPED_EXN, userMetadataMap, countError = false)

        val openTx = openTx(txKey, null)
        val result = try {
            val opts = SourceLogTxIndexer.TxOpts(
                txKey = txKey,
                currentTime = effectiveSystemTime,
                systemTime = effectiveSystemTime.asMicros,
                defaultTz = defaultTz,
                user = user,
            )
            sourceLogTxIndexer.ForTx(txOps, opts).indexTx(openTx)
        } catch (e: Throwable) {
            openTx.close(); throw e
        }

        when (result) {
            is TxResult.Committed ->
                try {
                    openTx.writeTxRow(null, userMetadataMap)
                    Committed(txKey) to openTx
                } catch (e: Throwable) {
                    openTx.close(); throw e
                }

            is TxResult.Aborted -> {
                LOG.debug(result.error) { "aborted tx" }
                // fresh tx for the abort row — the original openTx may hold partial writes
                openTx.close()
                commitStandaloneTx(txKey, result.error, userMetadataMap, countError = true)
            }
        }
    }

    private fun resolveTx(
        msgId: MessageId, record: Log.Record<SourceMessage>, msg: SourceMessage
    ): Pair<TransactionResult, OpenTx> {
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

    suspend fun cancelAndJoin() = termJob.cancelAndJoin()

    override fun close() {
        extSource?.close()
        replicaProducer.close()
        inFlight?.close() // free a batch whose settle never completed (fault / teardown)
        stagingIndex.close() // free any un-promoted staged slices before the allocator
        allocator.close() // last: Arrow won't close it while a child buffer is live
    }
}
