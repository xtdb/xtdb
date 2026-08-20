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
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.storage.Storage
import xtdb.compactor.Compactor
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

internal class LeaderLogProcessor(
    allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val partitionStorage: PartitionStorage,
    crashLogger: CrashLogger,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val driver: LeaderDriver,
    private val compactor: Compactor.ForDatabase,
    private val watchers: Watchers,
    private val extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    afterReplicaMsgId: MessageId,
    private val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    scope: CoroutineScope,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : LogProcessor.Processor<SourceMessage>, TxIndexer, Leadership {

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

    // The one apply path, shared with a node that is not leading. `leadership = this` is what leading
    // adds to it, and it reaches the applier through the two hooks below and nowhere else.
    private val applier = ReplicaApplier(
        allocator, "leader-log-processor", bufferPool, partitionState, dbName, compactor, watchers,
        dbCatalog, leadership = this,
        afterReplicaMsgId = afterReplicaMsgId,
        hasExternalSource = extSource != null,
        meterRegistry = nodeBase.meterRegistry,
    )

    val pendingBlock: PendingBlock? get() = applier.pendingBlock

    // The consume-back position: the last replica-log record we have read back and applied. Advances
    // in the apply loop; on demote it seeds the re-opened follower (with `pendingBlock`).
    override val latestReplicaMsgId: MessageId get() = applier.latestReplicaMsgId

    // Where the consume pump starts tailing (the transition replay-target). Distinct from the advancing
    // `latestReplicaMsgId` — the tail is opened once, from here.
    private val replayFrom: MessageId = afterReplicaMsgId

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

    // Records the consume pump has tailed back off the replica log, awaiting application.
    private val replicaMsgs = Channel<Log.Record<ReplicaMessage>>(capacity = 128)

    // Serialize each ResolvedTx (the costly Arrow-IPC step, kept off the resolver) and append it, in
    // order, through the driver. Plain (non-transactional) appends: the sole fence on a zombie leader is
    // now the term check on consume-back — a higher-term record read back means we've been superseded and
    // resign (see ReplicaApplier.apply) — replacing the Kafka transactional producer that fenced at
    // commit (#5817).
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

    // Tail our own replica log from the replay target and post everything back to the apply loop. The
    // same plain tail the follower uses — a separate consumer from the source-log group subscription that
    // drives leader election, so this doesn't interfere with it.
    private suspend fun consumePump() {
        driver.tailReplica(replayFrom) { records -> records.forEach { replicaMsgs.send(it) } }
    }

    // ---- the two hooks (see Leadership) ----

    override val term get() = leaderTerm

    /**
     * Everything we resolved in order to produce a record, answered from what we still hold rather than
     * re-derived from the record we get back.
     *
     * A boundary is the other hook's, and a `NoOp` carries nothing we hold — both fall through to the
     * ordinary path, which for a `NoOp` is the same watermark advance a non-leading node makes.
     */
    override suspend fun applyAuthored(record: Log.Record<ReplicaMessage>): Boolean =
        when (val msg = record.message) {
            is ReplicaMessage.ResolvedTx -> {
                commitResolvedHead(msg); true
            }

            // Catalog already updated on the resolve side; here we only advance the source watermark.
            is TriesAdded -> {
                watchers.notifyMsg(msg.sourceMsgId); true
            }

            // Our own upload, read back after uploadBlock already rolled the index — nothing to do but
            // advance the watermark.
            is ReplicaMessage.BlockUploaded -> {
                watchers.notifyMsg(msg.latestProcessedMsgId); true
            }

            // Catalog already updated on the resolve side (see handleTriesDeleted).
            is ReplicaMessage.TriesDeleted -> true

            is BlockBoundary, is ReplicaMessage.NoOp -> false
        }

    // Commit the transaction at the head of the resolver's queue — this record, by construction: we
    // append in resolution order and read back in position order, so the two are the same order. Its
    // relations are still ours, so nothing is re-materialised from the record.
    private suspend fun commitResolvedHead(msg: ReplicaMessage.ResolvedTx) {
        val head = txResolver.removeHead()
        // The check is inside the try: head is already off the queue, so teardown's failPending can't
        // reach it — the catch must fail its handle and the finally must free it, on ANY throw here
        // (a queue-head mismatch included), or we leak Arrow buffers and hang an awaiting executeTx.
        try {
            check(head.txKey.txId == msg.txId) {
                "[$dbName] queue head ${head.txKey.txId} != consumed tx ${msg.txId}"
            }
            driver.applyTx(head.txKey, head.allTables.associate { it.ref to it.relation })
            // dbOp (attach/detach) was already applied on the resolve side (it had to run to produce
            // the tx result), so it is ours to skip here and the applier's to do for everyone else.
            watchers.notifyTx(head.txResult, head.srcMsgId, head.externalSourceToken)
            head.pending?.complete(head.txResult)
        } catch (e: Throwable) {
            head.pending?.completeExceptionally(e)
            throw e
        } finally {
            head.close()
        }
    }

    /**
     * Take the cut, unconditionally: every boundary we read is one we wrote.
     *
     * The fence has already discarded anything below our term, and anything above it superseded us
     * before reaching here — so a boundary that is not ours is one we are no longer leading to see.
     * Declining is what a node with no leadership expresses by not having this hook at all.
     */
    override suspend fun takeCut(record: Log.Record<ReplicaMessage>, msg: BlockBoundary): Boolean {
        // liveIndex now holds exactly this block's txs (by log order); snapshot, upload the files,
        // append BlockUploaded and roll the index — all inside uploadBlock.
        driver.uploadBlock(record.msgId, leaderTerm, msg)

        blockGc.signal()
        trieGc.signal()

        blockInProgress = false
        resumeCh.trySend(Unit) // resume a source batch stashed by the pause, if any

        return true
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
                // We answer for it on our own consume-back (see applyAuthored); a node that did not
                // write it applies it from the record.
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
                watchers.notifyMsg(msgId)
                // Keep the resolve-side gauge in step with the watermark we just advanced, or a following
                // block cut would carry a lower latestProcessedMsgId and regress `notifyMsg` on apply.
                lastResolvedSrcMsgId = msgId
            }
        }
    }

    private suspend fun handleIndexTx(task: ExtSourceTask.IndexTx) {
        // The stamped watermark must be the RESOLVE-side one (`lastResolvedSrcMsgId`), not `watchers.*`:
        // watchers advance on consume-back (apply), which lags resolution, so a source tx
        // resolved-and-appended ahead of this ext tx would apply first and push the watermark past a stale
        // stamp — `notifyTx`'s monotonicity check would then fire (#5817).
        val resolvedTx = txResolver.indexTx(task.msg, srcMsgId = lastResolvedSrcMsgId)

        task.msg.externalSourceToken?.let { lastResolvedExtToken = it }
        appendTx(resolvedTx, lastResolvedSrcMsgId)
    }

    private suspend fun handleTriesDeleted(task: GcTask.TriesDeleted) {
        // Remove from the local catalog here, then replicate for followers — eager on the resolve side so
        // the GC's `commitTriesDeleted` await returns with the catalog already updated (its contract; the
        // GC has already deleted the files). Safe as a fenced-log projection for the same reason as
        // TriesAdded: the block-cut pause serialises this against any boundary, and gcCh is excluded while a
        // block is in progress. We answer for it on our own consume-back (see applyAuthored); a node that
        // did not write it applies it from the record.
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

    // The term handle: a supervisor child of the Database scope, owning the pumps + persister loop
    // (launched in `init`) and the GCs. A term-internal failure surfaces via `notifyError` rather than
    // cancelling the source-log subscription; `cancelAndJoin` reaps the whole term. See dev/doc/coroutines.adoc.
    private val termJob = SupervisorJob(scope.coroutineContext.job)

    // The GCs run under a SupervisorJob child of `termJob`, so one GC's failure cancels neither its sibling
    // nor the persister; cancelling `termJob` reaps them all.
    private val gcScope = scope + SupervisorJob(termJob)

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
        // Routed through the persister rather than applied inline: the catalog removal has to be serialised
        // against block cuts, and this await must not return until the catalog reflects it — which is the
        // GC's contract, since it has already deleted the files. See handleTriesDeleted.
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

    // Launched last so every field the body reaches — e.g. blockGc/trieGc via the boundary path — is
    // initialised before the first record. Runs under `termJob`, so `cancelAndJoin` reaps it.
    init {
        CoroutineScope(scope.coroutineContext + termJob).launch {
            // Core: the append pump, the consume pump and the persister loop, structured together so any
            // one failing cancels the others and surfaces the cause.
            launch {
                var cause: Throwable? = null
                try {
                    coroutineScope {
                        launch(CoroutineName("$dbName-append-pump")) { appendPump() }
                        launch(CoroutineName("$dbName-consume-pump")) { consumePump() }
                        persisterLoop()
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

    private suspend fun persisterLoop() {
        while (true) {
            val work = selectUnbiased<Work> {
                replicaMsgs.onReceive { Apply(it) }
                if (!blockInProgress) {
                    if (pausedBatch != null) resumeCh.onReceive { Resume }
                    driver.sourceBatches.onBatch { SourceWork(it) }
                    extSourceCh.onReceive { RunTask(it) }
                    gcCh.onReceive { RunTask(it) }
                }
            }

            when (work) {
                // The apply loop is where a supersession fails the term; let it propagate (interrupts too).
                is Apply ->
                    try {
                        applier.apply(work.record)
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
                    val pb = pausedBatch ?: continue
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

    suspend fun cancelAndJoin() = termJob.cancelAndJoin()

    override fun close() {
        extSource?.close()
        driver.close()
        applier.close()
        // Frees every resolved-but-not-applied tx — safe now that cancelAndJoin has joined the persister
        // and the pumps.
        txResolver.close()
    }
}
