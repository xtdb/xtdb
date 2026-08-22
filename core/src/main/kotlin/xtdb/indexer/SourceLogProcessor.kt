package xtdb.indexer

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectBuilder
import xtdb.api.DatabaseName
import xtdb.api.error.Anomaly
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

    private val bufferPool = partitionStorage.bufferPool
    private val blockCatalog = partitionState.blockCatalog
    private val trieCatalog = partitionState.trieCatalog

    private val blockFlusher = BlockFlusher(flushTimeout, blockCatalog)

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
                val cut = expectedBlockIdx != null && expectedBlockIdx == (blockCatalog.currentBlockIndex ?: -1L)

                if (cut) cutBlock(msgId)
                // see #5680
                else replicaAppender.append(ControlItem(ReplicaMessage.NoOp(srcMsgId = msgId, termId = leaderTerm)))

                txResolver.advanceSrcMsgId(msgId)
                cut
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

                appendTx(
                    if (error == null)
                        txResolver.indexDbOp(msgId, record.logTimestamp, DbOp.Attach(msg.dbName, msg.config))
                    else
                        txResolver.indexFailedDbOp(msgId, record.logTimestamp, error)
                )
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
    fun SelectBuilder<Unit>.selectWork() {
        if (pausedBatch != null) resumeCh.onReceive {
            val pb = pausedBatch ?: return@onReceive
            pausedBatch = null
            watchers.runTaskGuarded(pb.task.onComplete) { runBatch(pb.task, pb.nextIdx) }
        }

        driver.sourceBatches.onBatch { batch ->
            watchers.runTaskGuarded(batch.onComplete) { runBatch(batch, 0) }
        }
    }

    private suspend fun maybeFlushBlock() {
        if (blockFlusher.checkBlockTimeout(blockCatalog))
            driver.requestFlushBlock(blockCatalog.currentBlockIndex ?: -1)
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
        if (records.isNotEmpty()) driver.sourceBatches.submit(records).await()
    }

    /**
     * Fail everyone still waiting on the source log: whatever a block cut stashed, and the pipe itself.
     *
     * The pipe's own shutdown owns the must-be-a-cancellation rule ([SourceBatch.abandon]), because the
     * transport's poll thread both awaits and sends there.
     */
    fun shutdown(cause: Throwable) {
        pausedBatch?.task?.abandon(cause)
        driver.sourceBatches.shutdown(cause)
    }
}
