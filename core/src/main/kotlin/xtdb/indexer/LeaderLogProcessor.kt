@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.selectUnbiased
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.TransactionResult
import xtdb.api.log.*
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.database.*
import xtdb.api.error.Anomaly
import xtdb.api.error.Interrupted
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.trie.TrieKey
import xtdb.util.*
import xtdb.util.StringUtil.asLexHex
import java.time.*
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.types.MessageId

private val LOG = LeaderLogProcessor::class.logger

internal class LeaderLogProcessor(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val partitionStorage: PartitionStorage,
    crashLogger: CrashLogger,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val blockUploader: BlockUploader,
    private val watchers: Watchers,
    private val extSource: ExternalSource?,
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    afterReplicaMsgId: MessageId,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration = Duration.ofMinutes(5),
    scope: CoroutineScope,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : LogProcessor.Processor<SourceMessage>, TxIndexer {

    init {
        require((dbCatalog != null) == (dbName == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val partition = partitionStorage.partition
    private val sourceLog = partitionStorage.sourceLog
    private val bufferPool = partitionStorage.bufferPool
    private val liveIndex = partitionState.liveIndex

    private val blockCatalog = partitionState.blockCatalog
    private val trieCatalog = partitionState.trieCatalog

    // Resolves each source-log / attach-detach / ext-source tx and holds it — with every other
    // resolved-but-not-yet-durable tx — until we've committed it into the live index below. Driven only
    // from the persister coroutine, and freed in close() once that job is joined; see TxResolver.
    private val txResolver =
        TxResolver(allocator, nodeBase, partitionStorage, partitionState, dbName, crashLogger, skipTxs, instantSource)

    var pendingBlock: PendingBlock? = null
        private set

    override var latestReplicaMsgId: MessageId = afterReplicaMsgId
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    private val maxStagedRows = nodeBase.config.indexer.rowsPerBlock

    /**
     * The in-flight replica-log append: one fenced producer transaction carrying the sealed batch, run in
     * the background and yielding each tx's replica-log position. At most one — the single fenced producer
     * permits only one open transaction at a time, and holding it as a nullable field makes that invariant
     * structural rather than checked at every append site.
     *
     * The txs it carries come back in its result, so there's no second copy here to drift from the
     * resolver's — the resolver owns them throughout, and frees them at [TxResolver.applied].
     */
    private var inFlight: Deferred<List<Pair<ResolvedTx, Log.MessageMetadata>>>? = null

    // Seal whatever has accumulated and launch its replica-log append — each tx serializing itself into
    // a replica message (ResolvedTx.toReplicaMessage), all appended in one fenced producer transaction —
    // in the background, so the resolver keeps resolving while the serialize + producer commit run.
    // The persister settles it (settleAppend) once it completes. No-op if an append is already in flight
    // (the accumulating tail rides the next kick, at settle) or there's nothing staged.
    private fun kickAppend() {
        if (inFlight != null) return
        val txs = txResolver.seal() ?: return

        inFlight = appendScope.async {
            replicaProducer
                .withTx { tx ->
                    txs.map { it to tx.appendMessage(it.toReplicaMessage()) }
                }
                .map { it.first to it.second.await() }
        }
    }

    private suspend fun settle(appended: List<Pair<ResolvedTx, Log.MessageMetadata>>) {
        for ((resolvedTx, metadata) in appended) {
            liveIndex.commitTx(resolvedTx.txKey, resolvedTx.allTables.associate { it.ref to it.relation })
            latestReplicaMsgId = metadata.msgId
            watchers.notifyTx(resolvedTx.txResult, resolvedTx.srcMsgId, resolvedTx.externalSourceToken)
        }

        // Watchers-derived rather than the last tx's: the promote loop above has just notified every
        // tx in send order, so latestSourceMsgId equals the last tx's, and the token null-coalesces
        // to the batch's last non-null — for a mixed source-log/ext-source batch, the last tx could
        // be a source-log tx whose null token would drop the CDC resume point from the boundary.
        // Matches the FlushBlock path, which already cuts blocks from the watchers' view.
        if (liveIndex.isFull()) finishBlock(watchers.latestSourceMsgId, watchers.externalSourceToken)

        // Complete after the whole settle (promote loop + any block cut): signalling per-tx mid-loop
        // lets a caller race ahead of finishBlock, and a hard cancel then can orphan a BlockBoundary
        // with no BlockUploaded (the #5783 sim regression caught at 15/300).
        appended.forEach { (resolvedTx, _) -> resolvedTx.pending?.complete(resolvedTx.txResult) }
    }

    // The batch is released back to the resolver only once settle returns; on a settle fault — including a
    // teardown cancellation thrown out of its await while the append coroutine may still be serializing the
    // slices — it stays sealed, and close() frees it after cancelAndJoin has joined the append.
    private suspend fun settleAppend() {
        val append = inFlight ?: return
        settle(append.await())
        inFlight = null
        txResolver.applied()
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
        class IndexTx(val msg: ExtSourceMessage) : ExtSourceTask {
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
            if (task is ExtSourceTask.IndexTx) task.msg.pending.cancel()
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
            is SourceMessage.Tx -> {
                txResolver.indexTx(msgId, record.logTimestamp, msg)
                kickAppend()
            }

            is SourceMessage.LegacyTx -> {
                txResolver.indexTx(msgId, record.logTimestamp, msg)
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
                val error = if (dbCatalog != null) {
                    try {
                        dbCatalog.attach(msg.dbName, msg.config)
                        null
                    } catch (e: Anomaly.Caller) {
                        LOG.debug(e) { "[$dbName] leader: attach database '${msg.dbName}' failed at $msgId" }
                        e
                    }
                } else null

                if (error == null)
                    txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Attach(msg.dbName, msg.config))
                else
                    txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)

                drainStaging()
            }

            is SourceMessage.DetachDatabase -> {
                val error = if (dbCatalog != null) {
                    try {
                        dbCatalog.detach(msg.dbName)
                        null
                    } catch (e: Anomaly.Caller) {
                        LOG.debug(e) { "[$dbName] leader: detach database '${msg.dbName}' failed at $msgId" }
                        e
                    }
                } else null

                if (error == null)
                    txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Detach(msg.dbName))
                else
                    txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)

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
        // Ext-source txs don't advance `latestSourceMsgId` (driven by the source log) — they track progress
        // via `externalSourceToken` — but the resolver stamps the current watermark onto the replicated
        // record. Read here rather than inside the resolver: this is the leader's view of the source log.
        txResolver.indexTx(task.msg, srcMsgId = watchers.latestSourceMsgId)

        kickAppend()
        trySettleAppend()

        // Safety bound: never accumulate more than a block's worth of rows — a bursty source
        // must not grow staging without limit behind one slow commit (nor leave durability to
        // the ~5-min FlushBlock). Rows, not bytes: it's the dimension the block-sizing machinery
        // (isFull/rowsPerBlock) already manages, and the drain it triggers is ordinary
        // backpressure, not a failure.
        if (txResolver.unsealedRowCount > maxStagedRows) drainStaging()
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
            bufferPool, partitionState, dbName,
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
                        check(!txResolver.hasUnsealedTxs || inFlight != null) {
                            "staged txs with no in-flight append — nothing will kick them"
                        }

                        val work = selectUnbiased<PersisterWork> {
                            // onJoin, not onAwait: join fires on failure too, and settleAppend's own await
                            // rethrows the fault inside the try below, routing it through notifyError.
                            inFlight?.let { append -> append.onJoin { Settle } }
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
                    txResolver.failPending(pendingCause)

                    // A buffered-but-never-received task is invisible to both failPending (it was
                    // never staged) and onUndeliveredElement (close() doesn't visit buffered
                    // elements — only cancel() does), so a caller outside the term scope would
                    // await it forever.
                    while (true) {
                        val task = extSourceCh.tryReceive().getOrNull() ?: break
                        task.onComplete.completeExceptionally(pendingCause)
                        if (task is ExtSourceTask.IndexTx) task.msg.pending.completeExceptionally(pendingCause)
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

    override suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): TransactionResult =
        submitTx(externalSourceToken, systemTime, writer).await()

    override suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
        writer: suspend (OpenTx) -> TxResult,
    ): Deferred<TransactionResult> {
        val task = ExtSourceTask.IndexTx(ExtSourceMessage(externalSourceToken, systemTime, writer))
        // enqueue's send throws if the channel is closed (dead indexer) — that's the early-exit signal.
        // The returned handle is the message's `pending`, completed at settle once the tx is durably
        // replicated; a fire-and-forget caller may discard it, and an unrecoverable failure also closes the
        // channel with its cause, so the next `enqueue` throws it.
        enqueue(task)
        return task.msg.pending
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
        // Frees every resolved-but-not-applied tx, including a batch whose settle never completed
        // (fault / teardown) — safe now that cancelAndJoin has joined the persister and the append.
        txResolver.close()
    }
}
