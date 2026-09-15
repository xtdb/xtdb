package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.selects.SelectBuilder
import xtdb.api.DatabaseName
import xtdb.api.error.Anomaly
import xtdb.api.error.Conflict
import xtdb.api.error.NotFound
import xtdb.api.log.DbOp
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.TriesAdded
import xtdb.api.log.SourceMessage
import xtdb.database.Database
import xtdb.database.PartitionState
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
     * The transport's poll thread awaits this inside `processRecords`, and anything other than a
     * CancellationException escaping there unwinds `openGroupSubscription` into the Database scope's
     * `CoroutineExceptionHandler` — which calls `watchers.notifyError`, so a *clean* resignation would end
     * up poisoning queries and evicting the shared consumer. See #5817.
     */
    fun abandon() = onComplete.cancel()
}

/**
 * Run a resolution task, routing its failure onto its completion handle so that no caller hangs.
 *
 * A successful source batch completes its own handle, possibly deferred if a block cut paused it.
 */
private inline fun runTaskGuarded(onComplete: CompletableDeferred<Unit>, block: () -> Unit) =
    try {
        block()
    } catch (e: Throwable) {
        onComplete.completeExceptionally(e)
        throw e
    }

/**
 * The source log's side of a leader term: the flush timer, the batches the transport hands over, and what
 * each record in one resolves to.
 *
 * A batch is processed a record at a time and stops where a record cuts a block, because nothing may
 * interleave between a boundary and its upload.
 */
internal class SourceLogProcessor(
    partitionState: PartitionState,
    private val dbCatalog: Database.Catalog?, private val dbName: DatabaseName, private val leaderTerm: Long,
    private val logsDriver: LogProcessor.LogsDriver,
    private val txResolver: TxResolver,
    private val blockCutter: BlockCutter,
    private val replicaAppender: ReplicaLogAppender,
    flushTimeout: Duration,
) : Log.RecordProcessor<SourceMessage> {

    // capacity 1: the poll thread can deposit one batch ahead and read the next while the persister
    // works, bounding lookahead to ~2 batches.
    // Backpressure falls out of a full channel suspending the send.
    private val ch = Channel<SourceBatch>(capacity = 1, onUndeliveredElement = { it.abandon() })

    private val tableCatalog = partitionState.tableCatalog

    private val blockFlusher = BlockFlusher(flushTimeout, tableCatalog)

    // A source batch paused mid-way by a block cut: the task, and where to pick it up again. At most one —
    // the poll thread awaits each batch before sending the next, so only one is ever in flight; a nullable
    // field makes that structural. Holds the *task*, so its failure policy stays the task's own.
    private class PausedBatch(val task: SourceBatch, val nextIdx: Int) {
        fun shutdown() = task.abandon()
    }

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

    private suspend fun appendTx(resolvedTx: ResolvedTx): Boolean {
        blockCutter.addRows(resolvedTx)

        replicaAppender.append(TxItem(resolvedTx, leaderTerm))

        return blockCutter.isFull
    }

    /**
     * Resolve one source-log record, answering whether it cut a block.
     *
     * Everything the resolve side decides about a record is decided here, so this is the seam a test
     * drives — [processRecords] adds only the batch pipe, which no caller but the transport needs.
     */
    suspend fun handleRecord(record: Log.Record<SourceMessage>): Boolean {
        val msgId = record.msgId
        val msg = record.message
        LOG.trace { "[$dbName] leader: message $msgId (${msg::class.simpleName})" }

        return when (msg) {
            is SourceMessage.Tx -> appendTx(txResolver.indexTx(msgId, record.logTimestamp, msg))

            is SourceMessage.LegacyTx -> appendTx(txResolver.indexTx(msgId, record.logTimestamp, msg))

            is SourceMessage.FlushBlock -> {
                val expectedBlockIdx = msg.expectedBlockIdx
                val cut = expectedBlockIdx != null && expectedBlockIdx == (tableCatalog.currentBlockIndex ?: -1L)

                if (cut) blockCutter.cut(msgId, txResolver.resolvedExtToken)
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
                // Forwarded whatever its storage version: each node guards the add on the way back in,
                // and this message is also what carries the source watermark forward.
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
        }
    }

    // Process a batch from `startIdx`, stopping where a record cuts a block — stashing the remainder on
    // [pausedBatch] so the loop resumes it after the upload. Completes the task only when fully drained.
    private suspend fun runBatch(task: SourceBatch, startIdx: Int) {
        try {
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
        } catch (e: Throwable) {
            task.onComplete.completeExceptionally(e)
            throw e
        }
    }

    fun SelectBuilder<Unit>.armSelect() {
        if (pausedBatch != null)
            resumeCh.onReceive {
                pausedBatch?.let { pb ->
                    pausedBatch = null
                    runBatch(pb.task, pb.nextIdx)
                }
            }

        ch.onReceive { batch -> runBatch(batch, 0) }
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(tableCatalog))
            logsDriver.requestFlushBlock(tableCatalog.currentBlockIndex ?: -1)
    }

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        maybeFlushBlock()

        // Await the batch through the persister rather than firing and returning:
        //  - the persister resolves + hands off to the append pump on its own thread (heavy work off the
        //    poll thread);
        //  - blocking here until the batch is resolved keeps the poll loop and the persister roughly in
        //    step (channel cap 1 → ~2 batches of lookahead);
        //  - so a rebalance/transition under runBlocking doesn't pile up behind unbounded resolution (#5741).
        if (records.isNotEmpty())
            SourceBatch(records).also { ch.send(it) }.onComplete.await()
    }

    fun shutdown() {
        pausedBatch?.shutdown()
        ch.cancel()
    }
}
