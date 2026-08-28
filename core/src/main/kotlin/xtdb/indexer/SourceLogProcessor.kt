package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectBuilder
import xtdb.api.DatabaseName
import xtdb.api.TransactionResult
import xtdb.api.error.Anomaly
import xtdb.api.error.Conflict
import xtdb.api.error.NotFound
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.table.fromSchemaAndTable
import xtdb.types.MessageId
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.trace
import java.time.Duration

private val LOG = SourceLogProcessor::class.logger

/** One poll batch inbound from the transport, plus the handle its submitter awaits. */
internal class SourceBatch(val records: List<Log.Record<SourceMessage>>) {
    val onComplete = CompletableDeferred<Unit>()

    /**
     * Fail the awaiting submitter, because the term is going away without finishing this batch.
     *
     * Always a cancellation, whatever the term's cause. The transport's poll thread awaits this inside
     * `processRecords`, and anything other than a CancellationException escaping there unwinds
     * `openGroupSubscription` into the Database scope's `CoroutineExceptionHandler` — which calls
     * `watchers.notifyError`, so a *clean* resignation would end up poisoning queries and evicting the
     * shared consumer. The term's real fault, if any, is already on the watchers; we keep it as this
     * cancellation's cause for the logs. See #5817.
     */
    fun abandon(cause: Throwable?) = onComplete.cancel(cause.asCancellation())
}

/**
 * The inbound source-log pipe — the one edge that runs the other way, so the persister *pulls*
 * batches through a select clause rather than being pushed at.
 *
 * One owned object rather than three members on [SourceLogProcessor]: submitting, selecting and shutting
 * down are the three ends of a single pipe, and keeping them together is what stops them drifting.
 */
internal class SourceBatches {
    // capacity 1: the poll thread can deposit one batch ahead and read the next while the persister
    // works, bounding lookahead to ~2 batches. Backpressure falls out of a full channel suspending
    // the send.
    private val ch = Channel<SourceBatch>(capacity = 1, onUndeliveredElement = { it.abandon(null) })

    /** Armed by the persister's select, alongside its own channels. */
    val onBatch get() = ch.onReceive

    /** Hand a batch to the persister; the returned handle completes once it has been processed. */
    suspend fun submit(records: List<Log.Record<SourceMessage>>) =
        SourceBatch(records).also { ch.send(it) }.onComplete

    /**
     * Term teardown: no batch will be processed after this, and a later [submit] throws.
     *
     * Fails everyone still waiting — senders via the close cause, and whatever is still queued via
     * [SourceBatch.abandon]. Miss the queued ones and the symptom is a hang on the transport's poll
     * thread, which is also the sole servicer of the transport's unregister, so it wedges the whole
     * subscription teardown (#5711 / #5817).
     *
     * Close and drain, in that order: `close` alone doesn't visit buffered elements (only `cancel` does),
     * so a queued batch's submitter would wait forever; and closing first means no send can slip into a
     * buffer we've already drained. The close cause must be a cancellation too — it is what a later `send`
     * throws, and the poll thread sends here. Only safe on the persister's exit path: it is the sole
     * receiver, so nothing competes with these `tryReceive`s.
     */
    fun shutdown(cause: Throwable?) {
        ch.close(cause.asCancellation())
        while (true) (ch.tryReceive().getOrNull() ?: break).abandon(cause)
    }
}

/**
 * Run a resolution task, routing failures onto its completion handle (and any external-source result) so
 * that no caller hangs. Interrupts are shutdown signals, not ingestion faults, so they don't poison the
 * watchers. A successful source batch completes its own handle, possibly deferred if a block cut paused it.
 */
private inline fun runTaskGuarded(
    onComplete: CompletableDeferred<Unit>,
    extResult: CompletableDeferred<TransactionResult>? = null,
    block: () -> Unit,
) =
    try {
        block()
    } catch (e: Throwable) {
        if (!e.isShutdownSignal) extResult?.let { if (!it.isCompleted) it.completeExceptionally(e) }
        onComplete.completeExceptionally(e)
        throw e
    }

/**
 * The source log's side of a leader term: the flush timer, the batches the transport hands over, and what
 * each record in one resolves to.
 *
 * A batch is processed a record at a time and stops where a record cuts a block, because nothing may
 * interleave between a boundary and its upload. Both [appendTx] and [cutBlock] report a cut back, so the
 * pause is this processor's own state rather than a read of the term's.
 */
internal class SourceLogProcessor(
    private val driver: LeaderDriver,
    private val txResolver: TxResolver,
    partitionStorage: PartitionStorage,
    partitionState: PartitionState,
    private val watchers: Watchers,
    private val dbCatalog: Database.Catalog?,
    private val dbName: DatabaseName,
    private val leaderTerm: Long,
    private val replicaAppender: ReplicaLogAppender,
    flushTimeout: Duration,

    /** Stage a resolved tx for append, answering whether it filled the block. */
    private val appendTx: suspend (ResolvedTx) -> Boolean,

    /** Inject a block boundary covering the source log up to [MessageId], pausing resolution behind it. */
    private val cutBlock: suspend (MessageId) -> Unit,
) : Log.RecordProcessor<SourceMessage> {

    private val sourceBatches = SourceBatches()

    private val bufferPool = partitionStorage.bufferPool
    private val tableCatalog = partitionState.tableCatalog
    private val trieCatalog = partitionState.trieCatalog

    private val blockFlusher = BlockFlusher(flushTimeout, tableCatalog)

    // A source batch paused mid-way by a block cut: the task, and where to pick it up again. At most one —
    // the poll thread awaits each batch before sending the next, so only one is ever in flight; a nullable
    // field makes that structural. Holds the *task*, so its failure policy stays the task's own.
    private class PausedBatch(val task: SourceBatch, val nextIdx: Int)

    private var pausedBatch: PausedBatch? = null

    // Poked when a stashed [pausedBatch] becomes resumable. Conflated: at most one resume is pending, and
    // the select clause is gated on `pausedBatch != null` (and, by the term, on the block having landed).
    private val resumeCh = Channel<Unit>(Channel.CONFLATED)

    fun blockUploaded() {
        resumeCh.trySend(Unit)
    }

    /**
     * Ask whether a dbOp will be accepted, without performing it — null if it will.
     *
     * A caller fault resolves as an aborted tx carrying that error, so the log records the refusal and
     * every node reaches the same verdict from it. Anything else is ours, and fails the term.
     */
    // The catalog is mutated when a record is read back, so it shows the state before every dbOp this
    // term has queued. The last one queued for a name says what will be there when this op applies; with
    // none, the catalog's own state stands. Same layering as `resolvedTxs`, read for the catalog.
    private inline fun checkDbOp(
        msgId: MessageId, op: String, opDbName: DatabaseName, check: (Database.Catalog) -> Unit,
    ): Anomaly.Caller? =
        dbCatalog?.let { dbCatalog ->
            try {
                check(dbCatalog)
                null
            } catch (e: Anomaly.Caller) {
                LOG.debug(e) { "[$dbName] leader: $op database '$opDbName' refused at $msgId" }
                e
            }
        }

    // Resolve one source-log record, answering whether it cut a block.
    private suspend fun handleRecord(record: Log.Record<SourceMessage>): Boolean {
        val msgId = record.msgId
        val msg = record.message
        LOG.trace { "[$dbName] leader: message $msgId (${msg::class.simpleName})" }

        return when (msg) {
            is SourceMessage.Tx -> appendTx(txResolver.indexTx(msgId, record.logTimestamp, msg))

            is SourceMessage.LegacyTx -> appendTx(txResolver.indexTx(msgId, record.logTimestamp, msg))

            is SourceMessage.FlushBlock -> {
                val expectedBlockIdx = msg.expectedBlockIdx
                val cut = expectedBlockIdx != null && expectedBlockIdx == (tableCatalog.currentBlockIndex ?: -1L)

                if (cut) cutBlock(msgId)
                // see #5680
                else replicaAppender.append(ControlItem(ReplicaMessage.NoOp(srcMsgId = msgId, termId = leaderTerm)))

                txResolver.advanceSrcMsgId(msgId)
                cut
            }

            is SourceMessage.AttachDatabase -> {
                val error = checkDbOp(msgId, "attach", msg.dbName) {
                    when (txResolver.stagedDbOp(msg.dbName)) {
                        is DbOp.Attach ->
                            throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to msg.dbName))

                        is DbOp.Detach -> throw Conflict(
                            "Database is still being detached — retry once the previous detach has completed",
                            "xtdb/db-being-detached", mapOf("db-name" to msg.dbName)
                        )

                        null -> it.checkCanAttach(msg.dbName, msg.config)
                    }
                }

                appendTx(
                    if (error == null)
                        txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Attach(msg.dbName, msg.config))
                    else
                        txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)
                )
            }

            is SourceMessage.DetachDatabase -> {
                val error = checkDbOp(msgId, "detach", msg.dbName) {
                    when (txResolver.stagedDbOp(msg.dbName)) {
                        // The primary is always held, so it never has one of these; anything that does
                        // will be there to detach by the time this op is applied.
                        is DbOp.Attach -> {}

                        is DbOp.Detach ->
                            throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to msg.dbName))

                        null -> it.checkCanDetach(msg.dbName)
                    }
                }

                appendTx(
                    if (error == null)
                        txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Detach(msg.dbName))
                    else
                        txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)
                )
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

                replicaAppender.append(
                    ControlItem(
                        TriesAdded(
                            msg.storageVersion, msg.storageEpoch, msg.tries,
                            sourceMsgId = msgId, termId = leaderTerm
                        )
                    )
                )
                txResolver.advanceSrcMsgId(msgId)
                false
            }

            // TODO this one's going after 2.2
            is SourceMessage.BlockUploaded -> {
                watchers.notifyApplied(null, msgId)
                // Keep the resolve-side watermark in step with the one we just advanced, or a following
                // block cut would carry a lower latestProcessedMsgId and regress it on apply.
                txResolver.advanceSrcMsgId(msgId)
                false
            }
        }
    }

    // Process a batch from `startIdx`, stopping where a record cuts a block — stashing the remainder on
    // [pausedBatch] so the loop resumes it after the upload. Completes the task only when fully drained.
    private suspend fun runBatch(task: SourceBatch, startIdx: Int) {
        var i = startIdx
        while (i < task.records.size) {
            val cutBlock = handleRecord(task.records[i])
            i++
            if (cutBlock) {
                pausedBatch = PausedBatch(task, i)
                return
            }
        }
        task.onComplete.complete(Unit)
    }

    /** Arm this processor's clauses. The term arms them only while no block cut is in progress. */
    fun SelectBuilder<Unit>.armSelect() {
        if (pausedBatch != null)
            resumeCh.onReceive {
                val pb = pausedBatch ?: return@onReceive
                pausedBatch = null
                runTaskGuarded(pb.task.onComplete) { runBatch(pb.task, pb.nextIdx) }
            }

        sourceBatches.onBatch { batch ->
            runTaskGuarded(batch.onComplete) { runBatch(batch, 0) }
        }
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(tableCatalog))
            driver.requestFlushBlock(tableCatalog.currentBlockIndex ?: -1)
    }

    /** The transport's edge: hand a poll batch over and await its resolution. */
    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        // Await the batch through the persister rather than firing and returning:
        //  - the persister resolves + hands off to the append pump on its own thread (heavy work off the
        //    poll thread);
        //  - blocking here until the batch is resolved keeps the poll loop and the persister roughly in
        //    step (channel cap 1 → ~2 batches of lookahead);
        //  - so a rebalance/transition under runBlocking doesn't pile up behind unbounded resolution (#5741).
        if (records.isNotEmpty()) sourceBatches.submit(records).await()
    }

    /**
     * Fail everyone still waiting on the source log: whatever a block cut stashed, and the pipe itself.
     *
     * The pipe's own shutdown owns the must-be-a-cancellation rule ([SourceBatch.abandon]), because the
     * transport's poll thread both awaits and sends there.
     */
    fun shutdown(cause: Throwable) {
        pausedBatch?.task?.abandon(cause)
        sourceBatches.shutdown(cause)
    }
}
