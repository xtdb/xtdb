@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.supervisorScope
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.TransactionResult
import xtdb.api.log.*
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
import java.time.*
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.types.MessageId

private val LOG = LeaderLogProcessor::class.logger

/**
 * A higher-term record read back on our own replica log: a newer leader has superseded us. Thrown from
 * the apply loop to fail the term cleanly (not a query-facing fault, so it doesn't poison the watchers);
 * the transport re-follows on the next rebalance. See #5817.
 */
private class LeaderSupersededException(message: String) : RuntimeException(message)

internal class LeaderLogProcessor(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val partitionStorage: PartitionStorage,
    crashLogger: CrashLogger,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val driver: LeaderDriver,
    private val watchers: Watchers,
    private val extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    private val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : LogProcessor.Processor<SourceMessage>, Role, TxIndexer {

    init {
        require((dbCatalog != null) == (dbName == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val partition = partitionStorage.partition
    private val bufferPool = partitionStorage.bufferPool
    private val liveIndex = partitionState.liveIndex

    private val blockCatalog = partitionState.blockCatalog
    private val trieCatalog = partitionState.trieCatalog

    override val latestBlock get() = blockCatalog.latestBlock

    // Resolves each source-log / attach-detach / ext-source tx and holds it — with every other
    // resolved-but-not-yet-applied tx — until we've read it back off our own replica log and committed it
    // into the live index. Driven only from the persister coroutine, and freed in close() once that job is
    // joined; see TxResolver.
    private val txResolver =
        TxResolver(allocator, nodeBase, partitionStorage, partitionState, dbName, crashLogger, skipTxs, instantSource)

    var pendingBlock: PendingBlock? = null
        private set

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

    // From the live index, not the node config: the two agree in production, but they are one value and
    // the live index is what owns the block being filled — `blockRowCount` below seeds the gauge from it.
    private val rowsPerBlock = liveIndex.rowsPerBlock

    // Rows in the current block so far (resolve-side gauge). The boundary is cut off this rather than
    // liveIndex.isFull(), which lags (it only reflects APPLIED — consume-back — txs). Seeded from the rows
    // already applied into the open block: on a leadership change the new leader inherits a partially-filled
    // block from replay, and must cut it at the same point the old leader would have (else block sizes drift
    // across restarts — the #5817 stop/start off-by-one). Reset when a boundary is injected.
    private var rowsSinceBlock: Long = liveIndex.blockRowCount

    // The source-log watermark and ext-source token as of the last-resolved tx — the boundary is now cut
    // on the resolve side, ahead of the watchers (which advance on apply), so its `latestProcessedMsgId`
    // and token come from here rather than `watchers.*`.
    private var lastResolvedSrcMsgId: MessageId = watchers.latestSourceMsgId
    private var lastResolvedExtToken: ExternalSourceToken? = watchers.externalSourceToken

    // A block cut is in progress: the BlockBoundary has been appended but its BlockUploaded has not yet
    // been produced. Resolution (source/ext/gc) is paused — excluded from the select — so no tx interleaves
    // between the boundary and its upload, keeping the follower's bounded pending-block buffer empty.
    private var blockInProgress: Boolean = false

    // ---- append pump ----

    // What the append pump serializes-and-appends, in resolution order. A tx borrows its [ResolvedTx]
    // (the resolver owns and frees it); a control message is appended verbatim.
    private sealed interface AppendItem
    private class TxItem(val resolvedTx: ResolvedTx) : AppendItem
    private class ControlItem(val message: ReplicaMessage) : AppendItem

    // Unbounded, on purpose:
    //  - the persister sends here from the same coroutine that services `replicaMsgs`;
    //  - a bounded channel could block that send, stalling the apply loop;
    //  - but apply is what drains the queue and makes progress → single-coroutine deadlock.
    // Backpressure comes from the block pause + the resolve-side row gauge, not channel capacity.
    private val awaitingAppend = Channel<AppendItem>(Channel.UNLIMITED)

    // Records read back off the replica log, awaiting application. The partition's reader fills it
    // through [queueReplicaMessage]; its capacity is what bounds how far that reader may run ahead.
    private val replicaMsgs = Channel<Log.Record<ReplicaMessage>>(capacity = 128)

    // Serialize each ResolvedTx (the costly Arrow-IPC step, kept off the resolver) and append it, in
    // order, through the driver. Plain (non-transactional) appends: the sole fence on a zombie leader is
    // now the term check on consume-back — a higher-term record read back means we've been superseded and
    // resign (see applyRecord) — replacing the Kafka transactional producer that fenced at commit (#5817).
    // An append failure propagates and tears the term down. Never frees a borrowed ResolvedTx — the
    // resolver owns it.
    private suspend fun appendPump() {
        for (item in awaitingAppend) {
            val msg = when (item) {
                is TxItem -> item.resolvedTx.toReplicaMessage(leaderTerm)
                is ControlItem -> item.message
            }
            driver.appendToReplica(msg)
        }
    }

    /**
     * Hand a record read back off the replica log to the apply loop, suspending while the loop is behind.
     *
     * Confirmation, not delivery: a leader learns its own writes landed by reading them back (#5817), so
     * every record reaches this — its own, and a superseding leader's alike.
     */
    suspend fun queueReplicaMessage(record: Log.Record<ReplicaMessage>) = replicaMsgs.send(record)

    // ---- apply loop (consume-back) ----

    // Apply one record read back off our own replica log. By term:
    //  - term > ours: a newer leader has superseded us → resign (fail the term).
    //  - term < ours: shouldn't appear past our replay target; discard defensively, still advancing.
    //  - term == ours (the common case; terms are unique per leader, so an equal-term record IS our own):
    //      - ResolvedTx  → import from the resolver's head (we still hold its relations — no re-materialisation).
    //      - BlockBoundary → trigger the block upload (liveIndex now holds exactly this block's txs).
    //      - everything else mirrors the follower.
    private suspend fun applyRecord(record: Log.Record<ReplicaMessage>) {
        val msg = record.message
        val term = msg.termId

        if (term > leaderTerm)
            throw LeaderSupersededException("[$dbName] superseded: read term $term > our term $leaderTerm at ${record.msgId}")

        // Below our term should not appear past our replay target; discard defensively, still advancing.
        if (term != 0L && term < leaderTerm) {
            LOG.debug { "[$dbName] leader: discarding stale-term record ${record.msgId} (term $term < $leaderTerm)" }
            watchers.notifyApplied(record.msgId)
            return
        }

        when (msg) {
            is ReplicaMessage.ResolvedTx -> {
                val head = txResolver.removeHead()
                // check is inside the try: head is already off the queue, so teardown's failPending can't
                // reach it — the catch must fail its handle and the finally must free it, on ANY throw here
                // (a queue-head mismatch included), or we leak Arrow buffers and hang an awaiting executeTx.
                try {
                    check(head.txKey.txId == msg.txId) {
                        "[$dbName] queue head ${head.txKey.txId} != consumed tx ${msg.txId}"
                    }
                    driver.applyTx(head.txKey, head.allTables.associate { it.ref to it.relation })
                    // dbOp (attach/detach) was already applied on the resolve side (it had to run to
                    // produce the tx result); the follower/transition apply it on consume-back, we don't.
                    watchers.notifyApplied(record.msgId, head.srcMsgId, head.txResult, head.externalSourceToken)
                    head.pending?.complete(head.txResult)
                } catch (e: Throwable) {
                    head.pending?.completeExceptionally(e)
                    throw e
                } finally {
                    head.close()
                }
            }

            // Catalog already updated on the resolve side; here we only advance the source watermark.
            is ReplicaMessage.TriesAdded -> watchers.notifyApplied(record.msgId, msg.sourceMsgId)

            is BlockBoundary -> {
                pendingBlock = PendingBlock(record.msgId, msg)
                // liveIndex now holds exactly this block's txs (by log order); snapshot, upload the files,
                // append BlockUploaded and roll the index — all inside uploadBlock.
                driver.uploadBlock(record.msgId, leaderTerm, msg)
                pendingBlock = null

                // the block's covered source position, as the follower does
                watchers.notifyApplied(record.msgId, msg.latestProcessedMsgId)

                blockGc.signal()
                trieGc.signal()

                blockInProgress = false
                resumeCh.trySend(Unit) // resume a source batch stashed by the pause, if any
            }

            // Our own BlockUploaded, read back after uploadBlock already rolled the index — nothing to do
            // but advance the watermark.
            is ReplicaMessage.BlockUploaded -> watchers.notifyApplied(record.msgId, msg.latestProcessedMsgId)

            is ReplicaMessage.NoOp -> watchers.notifyApplied(record.msgId, msg.srcMsgId)

            // Catalog already updated on the resolve side (see handleTriesDeleted); nothing to do here.
            is ReplicaMessage.TriesDeleted -> watchers.notifyApplied(record.msgId)
        }
    }

    // ---- resolution ----

    // Cut a block: inject the boundary (in resolution order, so it lands after this block's txs and before
    // the next block's) and pause resolution until it is read back and uploaded. Reset the row gauge.
    private suspend fun cutBlock(latestProcessedMsgId: MessageId, externalSourceToken: ExternalSourceToken?) {
        val boundary = BlockBoundary(
            (blockCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, externalSourceToken, termId = leaderTerm
        )
        awaitingAppend.send(ControlItem(boundary))
        blockInProgress = true
        rowsSinceBlock = 0
    }

    // Hand a freshly-resolved tx to the append pump, and cut a block if this tx filled one.
    //
    // [srcMsgId] is the source-log position this tx sits at. A source-log tx advances it; an ext-source tx
    // passes the current one back, because it has no source-log position of its own — which is exactly what
    // gets stamped on its replica record.
    private suspend fun appendTx(resolvedTx: ResolvedTx, srcMsgId: MessageId) {
        lastResolvedSrcMsgId = srcMsgId
        rowsSinceBlock += resolvedTx.allTables.sumOf { it.relation.rowCount.toLong() }
        awaitingAppend.send(TxItem(resolvedTx))
        if (rowsSinceBlock >= rowsPerBlock) cutBlock(lastResolvedSrcMsgId, lastResolvedExtToken)
    }

    private suspend fun handleSourceLogRecord(record: Log.Record<SourceMessage>) {
        val msgId = record.msgId
        val msg = record.message
        LOG.trace { "[$dbName] leader: message $msgId (${msg::class.simpleName})" }

        when (msg) {
            is SourceMessage.Tx -> appendTx(txResolver.indexTx(msgId, record.logTimestamp, msg), msgId)

            is SourceMessage.LegacyTx -> appendTx(txResolver.indexTx(msgId, record.logTimestamp, msg), msgId)

            is SourceMessage.FlushBlock -> {
                val expectedBlockIdx = msg.expectedBlockIdx
                if (expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)) {
                    cutBlock(msgId, lastResolvedExtToken)
                } else {
                    // see #5680
                    awaitingAppend.send(ControlItem(ReplicaMessage.NoOp(srcMsgId = msgId, termId = leaderTerm)))
                }
                lastResolvedSrcMsgId = msgId
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

                val resolvedTx =
                    if (error == null)
                        txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Attach(msg.dbName, msg.config))
                    else
                        txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)

                appendTx(resolvedTx, msgId)
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

                val resolvedTx =
                    if (error == null)
                        txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Detach(msg.dbName))
                    else
                        txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)

                appendTx(resolvedTx, msgId)
            }

            is SourceMessage.TriesAdded -> {
                // Mutate the local trie catalog here (as the leader did pre-rewrite), then replicate for
                // followers. Eager on the resolve side, not deferred to our own consume-back, because:
                //  - callers see the effect promptly (the compactor/GC read the catalog synchronously);
                //  - it stays a projection of the fenced log anyway — the block-cut pause serialises trie
                //    mutations against boundaries, so no block snapshot straddles this add.
                // We skip re-applying it on our own consume-back (see applyRecord); the follower applies it.
                if (msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch)
                    msg.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                        trieCatalog.addTries(fromSchemaAndTable(tableName), tries, record.logTimestamp)
                    }
                awaitingAppend.send(
                    ControlItem(TriesAdded(msg.storageVersion, msg.storageEpoch, msg.tries, sourceMsgId = msgId, termId = leaderTerm))
                )
                lastResolvedSrcMsgId = msgId
            }

            // TODO this one's going after 2.2
            is SourceMessage.BlockUploaded -> {
                watchers.notifyApplied(null, msgId)
                // Keep the resolve-side gauge in step with the watermark we just advanced, or a following
                // block cut would carry a lower latestProcessedMsgId and regress it on apply.
                lastResolvedSrcMsgId = msgId
            }
        }
    }

    private suspend fun handleIndexTx(task: ExtSourceTask.IndexTx) {
        // The stamped watermark must be the RESOLVE-side one (`lastResolvedSrcMsgId`), not `watchers.*`:
        // watchers advance on consume-back (apply), which lags resolution, so a source tx
        // resolved-and-appended ahead of this ext tx would apply first and push the watermark past a stale
        // stamp — `notifyApplied`'s monotonicity check would then fire (#5817).
        val resolvedTx = txResolver.indexTx(task.msg, srcMsgId = lastResolvedSrcMsgId)

        task.msg.externalSourceToken?.let { lastResolvedExtToken = it }
        appendTx(resolvedTx, lastResolvedSrcMsgId)
    }

    private suspend fun handleTriesDeleted(task: GcTask.TriesDeleted) {
        // Remove from the local catalog here, then replicate for followers — eager on the resolve side so
        // the GC's `commitTriesDeleted` await returns with the catalog already updated (its contract; the
        // GC has already deleted the files). Safe as a fenced-log projection for the same reason as
        // TriesAdded: the block-cut pause serialises this against any boundary, and gcCh is excluded while a
        // block is in progress. Skipped on our own consume-back (see applyRecord); the follower applies it.
        trieCatalog.deleteTries(task.tableName, task.trieKeys)
        awaitingAppend.send(
            ControlItem(ReplicaMessage.TriesDeleted(task.tableName.schemaAndTable, task.trieKeys, termId = leaderTerm))
        )
    }

    // ---- persister channels & loop ----

    private sealed interface PersisterTask {
        val onComplete: CompletableDeferred<Unit>

        /**
         * Fail this task's awaiting caller, because the term is going away without finishing it.
         *
         * The *kind* of failure — cancellation vs the term's real cause — depends on who awaits the handle,
         * so it belongs here, on the task, rather than being re-decided at each teardown site. Callers just
         * abandon whatever they hold.
         */
        fun abandon(cause: Throwable)
    }

    private sealed interface ExtSourceTask : PersisterTask {
        class IndexTx(val msg: ExtSourceMessage) : ExtSourceTask {
            override val onComplete = CompletableDeferred<Unit>()

            // The real cause: this is an ext-source caller's own tx, awaiting its own result, and it isn't
            // on the transport's poll thread — so it both wants and can safely see why the term died.
            override fun abandon(cause: Throwable) {
                onComplete.completeExceptionally(cause)
                msg.pending.completeExceptionally(cause)
            }
        }
    }

    private sealed interface GcTask : PersisterTask {
        data class TriesDeleted(val tableName: TableRef, val trieKeys: Set<TrieKey>) : GcTask {
            override val onComplete = CompletableDeferred<Unit>()

            override fun abandon(cause: Throwable) {
                onComplete.completeExceptionally(cause)
            }
        }
    }

    // Undelivered — a cancelled send, or a cancelled channel — is a term-teardown failure like any other,
    // so it goes through the task's own `abandon` rather than a second, hand-rolled policy here.
    private fun <T : PersisterTask> persisterChannel(capacity: Int) =
        Channel<T>(capacity, onUndeliveredElement = { it.abandon(CancellationException("leader term closed")) })

    // capacity 1 so a fire-and-forget `submitTx` caller can queue one tx ahead while the persister works
    // the current one. `executeTx` still blocks on the result regardless of capacity.
    private val extSourceCh = persisterChannel<ExtSourceTask>(capacity = 1)

    private val gcCh = persisterChannel<GcTask>(Channel.UNLIMITED)

    /**
     * Shut this channel down and fail everyone still waiting on it: senders (via the close cause) and
     * whatever is still queued (via each task's [PersisterTask.abandon]).
     *
     * Close and drain are bundled because both are needed and the order matters — `close` alone doesn't
     * visit buffered elements (only `cancel` does), so a queued task's caller would wait forever; and
     * closing *first* means no send can slip into a buffer we've already drained.
     *
     * Only safe on the persister's own exit path: it is the sole receiver, so nothing competes with these
     * `tryReceive`s.
     */
    private fun <T : PersisterTask> Channel<T>.shutdown(cause: Throwable) {
        close(cause)
        while (true) (tryReceive().getOrNull() ?: break).abandon(cause)
    }

    // A source batch paused mid-way by a block cut: the task, and where to pick it up again. At most one —
    // the poll thread awaits each batch before sending the next, so only one is ever in flight; a nullable
    // field makes that structural. Holds the *task*, so its failure policy stays the task's own.
    private class PausedBatch(val task: SourceBatch, val nextIdx: Int)

    private var pausedBatch: PausedBatch? = null

    // Poked when a stashed [pausedBatch] becomes resumable (the block finished uploading). Conflated: at
    // most one resume is pending, and the select clause is gated on `pausedBatch != null && !blockInProgress`.
    private val resumeCh = Channel<Unit>(Channel.CONFLATED)

    // Process a source batch from `startIdx`, stopping if a block cut pauses us — stashing the remainder on
    // [pausedBatch] so the loop resumes it after the upload. Completes the task only when fully drained.
    private suspend fun runSourceBatch(task: SourceBatch, startIdx: Int) {
        var i = startIdx
        while (i < task.records.size) {
            handleSourceLogRecord(task.records[i])
            i++
            if (blockInProgress) {
                pausedBatch = PausedBatch(task, i)
                return
            }
        }
        task.onComplete.complete(Unit)
    }

    private sealed interface Work
    private class Apply(val record: Log.Record<ReplicaMessage>) : Work
    private class SourceWork(val batch: SourceBatch) : Work
    private class RunTask(val task: PersisterTask) : Work
    private data object Resume : Work

    internal val blockGc = nodeBase.config.garbageCollector.let { cfg ->
        BlockGarbageCollector(
            bufferPool, blockCatalog,
            blocksToKeep = cfg.blocksToKeep,
            enabled = cfg.enabled,
            meterRegistry = nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
            dbName = dbName
        )
    }

    internal val trieGc = nodeBase.config.garbageCollector.let { cfg ->
        // Routed through the persister rather than applied inline: the catalog removal has to be serialised
        // against block cuts, and this await must not return until the catalog reflects it — which is the
        // GC's contract, since it has already deleted the files. See handleTriesDeleted.
        val commitTriesDeleted: suspend (TableRef, Set<TrieKey>) -> Unit = { tableName, trieKeys ->
            enqueue(GcTask.TriesDeleted(tableName, trieKeys)).await()
        }

        TrieGarbageCollector(
            bufferPool, partitionState, dbName,
            commitTriesDeleted, cfg.blocksToKeep, cfg.garbageLifetime,
            cfg.enabled,
            nodeBase.meterRegistry,
            dispatcher = gcDispatcher,
        )
    }

    override fun armWork(select: SelectBuilder<suspend () -> Unit>) {
        with(select) {
            replicaMsgs.onReceive { r -> { runWork(Apply(r)) } }
            if (!blockInProgress) {
                if (pausedBatch != null) resumeCh.onReceive { { runWork(Resume) } }
                driver.sourceBatches.onBatch { b -> { runWork(SourceWork(b)) } }
                extSourceCh.onReceive { t -> { runWork(RunTask(t)) } }
                gcCh.onReceive { t -> { runWork(RunTask(t)) } }
            }
        }
    }

    private suspend fun runWork(work: Work) {
        when (work) {
            // Applying is where a supersession fails the term; let it propagate (interrupts too).
            is Apply ->
                try {
                    applyRecord(work.record)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: LeaderSupersededException) {
                    throw e
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Interrupted) {
                    throw e
                } catch (e: Throwable) {
                    watchers.notifyError(e)
                    throw e
                }

            is Resume -> {
                val pb = pausedBatch ?: return
                pausedBatch = null
                runTaskGuarded(pb.task.onComplete) { runSourceBatch(pb.task, pb.nextIdx) }
            }

            // The batch completes its own onComplete (deferred, if a block cut pauses it).
            is SourceWork ->
                runTaskGuarded(work.batch.onComplete) { runSourceBatch(work.batch, 0) }

            is RunTask -> {
                val task = work.task
                runTaskGuarded(task.onComplete, extResult = (task as? ExtSourceTask.IndexTx)?.msg?.pending) {
                    when (task) {
                        is ExtSourceTask.IndexTx -> {
                            handleIndexTx(task); task.onComplete.complete(Unit)
                        }
                        is GcTask.TriesDeleted -> {
                            handleTriesDeleted(task); task.onComplete.complete(Unit)
                        }
                    }
                }
            }
        }
    }

    // Completed by [workFailed] with the cause. Write-once, and the only thing the term learns about the
    // loop it no longer runs.
    private val termFailure = CompletableDeferred<Throwable>()

    /**
     * End this term, because the work loop it no longer runs has failed with [cause].
     *
     * The logging, the watchers and the sweep of everything staged are still the term's, and so is the
     * distinction they turn on — a supersession is a clean resignation where an append fault is not — so
     * the cause has to come back here to reach them.
     */
    override fun workFailed(cause: Throwable) {
        termFailure.complete(cause)
    }

    /**
     * Run this term until it is cancelled or fails: the append pump, the collectors and the ext source.
     * Cancelling the caller is what ends the term.
     */
    suspend fun runTerm(): Unit = coroutineScope {
        launch {
            supervisorScope {
                launch { blockGc.run() }
                launch { trieGc.run() }
            }
        }

        // Core: the append pump, and the term's own end. Structured together so a pump failure and a
        // loop failure both arrive here, and either cancels the other.
        launch {
            var cause: Throwable? = null
            try {
                coroutineScope {
                    launch(CoroutineName("$dbName-append-pump")) { appendPump() }
                    throw termFailure.await()
                }
            } catch (_: CancellationException) {
                // term cancellation
            } catch (e: LeaderSupersededException) {
                // superseded by a newer leader — expected, not a query-facing fault; the transport
                // re-follows on the next rebalance. Don't poison the watchers.
                LOG.info("[$dbName] ${e.message}")
                cause = e
            } catch (t: Throwable) {
                // A genuine term fault (e.g. an append-pump commit fault) surfaces to queries as a
                // failed term. Idempotent — the apply arm may already have notified for its own faults.
                LOG.error(t) { "[$dbName] leader term failed" }
                cause = t
                watchers.notifyError(t)
            } finally {
                val pendingCause = cause ?: CancellationException("leader term closed")

                // Nothing may be left awaiting the persister once it has gone: whatever is staged,
                // paused, or still queued gets failed here. Each task's own `abandon` picks the failure
                // *kind*, so this is a flat sweep with no per-caller special-casing.
                //
                // Miss anything and the symptom is a hang, not an error — and for a source-log batch
                // that hang is on the transport's poll thread (inside `processRecords`), which is also
                // the sole servicer of the transport's unregister. So it wedges the whole subscription
                // teardown and blows DatabaseCatalog.close's bound (#5711 / #5817).
                txResolver.failPending(pendingCause)
                pausedBatch?.task?.abandon(pendingCause)

                // The source-log pipe lives on the driver; its shutdown applies the same close-and-drain,
                // and owns the must-be-a-cancellation rule (SourceBatch.abandon) because the poll thread
                // both awaits and sends there.
                driver.sourceBatches.shutdown(pendingCause)
                extSourceCh.shutdown(pendingCause)
                gcCh.shutdown(pendingCause)

                // The partition's reader is suspended on a send here rather than awaiting a task, so a
                // close is all it needs — as a cancellation, since it is the reader's own coroutine that
                // sees it and a benign teardown must not poison the watchers.
                replicaMsgs.close(pendingCause.asCancellation())
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

    // Run a resolution task, routing failures onto its completion handle (and any ext-source result) so no
    // caller hangs. Interrupts are shutdown signals, not ingestion faults, so they don't poison the watchers.
    // A successful source batch completes its onComplete itself (possibly deferred, if paused).
    private suspend inline fun runTaskGuarded(
        onComplete: CompletableDeferred<Unit>,
        extResult: CompletableDeferred<TransactionResult>? = null,
        block: () -> Unit,
    ) {
        try {
            block()
        } catch (e: CancellationException) {
            if (!onComplete.isCompleted) onComplete.cancel(e)
            throw e
        } catch (e: InterruptedException) {
            if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
            throw e
        } catch (e: Interrupted) {
            if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
            if (!onComplete.isCompleted) onComplete.completeExceptionally(e)
            extResult?.let { if (!it.isCompleted) it.completeExceptionally(e) }
            throw e
        }
    }

    // Hand the task to the persister and return its completion handle. The caller decides whether to await
    // it: `executeTx`, GC and `processRecords` await; `submitTx` doesn't. Suspends only on the channel send.
    private suspend fun enqueue(task: PersisterTask): Deferred<Unit> {
        when (task) {
            is ExtSourceTask -> extSourceCh.send(task)
            is GcTask -> gcCh.send(task)
        }
        return task.onComplete
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
        // enqueue's send throws if the channel is closed (dead indexer) — the early-exit signal. The
        // returned handle is the message's `pending`, completed on consume-back once the tx is durably
        // replicated AND confirmed unfenced (ReadIndex); an unrecoverable failure also closes the channel
        // with its cause, so the next `enqueue` throws it.
        enqueue(task)
        return task.msg.pending
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(blockCatalog))
            driver.requestFlushBlock(blockCatalog.currentBlockIndex ?: -1)
    }

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        // Await the batch through the persister rather than firing and returning:
        //  - the persister resolves + hands off to the append pump on its own thread (heavy work off the
        //    poll thread);
        //  - blocking here until the batch is resolved keeps the poll loop and the persister roughly in
        //    step (channel cap 1 → ~2 batches of lookahead);
        //  - so a rebalance/transition under runBlocking doesn't pile up behind unbounded resolution (#5741).
        if (records.isNotEmpty()) driver.sourceBatches.submit(records).await()
    }

    override fun close() {
        extSource?.close()
        driver.close()
        // Frees every resolved-but-not-applied tx — safe only once the term's job has been joined, so the
        // persister and the pumps are gone.
        txResolver.close()
    }
}
