@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.log.*
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.database.*
import xtdb.api.error.Interrupted
import xtdb.api.log.ReplicaMessage
import xtdb.util.*
import java.time.*
import xtdb.api.tx.ExternalSource
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
    nodeBase: NodeBase,
    partitionStorage: PartitionStorage,
    crashLogger: CrashLogger,
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val driver: LeaderDriver,
    private val watchers: Watchers,

    private val replicaAppender: ReplicaLogAppender,

    private val extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    private val dbCatalog: Database.Catalog?,
    private val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : LogProcessor.Processor<SourceMessage>, Role {

    init {
        require((dbCatalog != null) == (dbName == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val partition = partitionStorage.partition
    private val liveIndex = partitionState.liveIndex

    private val blockCatalog = partitionState.blockCatalog

    // Resolves each source-log / attach-detach / ext-source tx and holds it — with every other
    // resolved-but-not-yet-applied tx — until we've read it back off our own replica log and committed it
    // into the live index. Driven only from the persister coroutine, and freed in close() once that job is
    // joined; see TxResolver.
    private val txResolver =
        TxResolver(
            allocator, nodeBase, partitionStorage, partitionState, dbName, crashLogger, skipTxs,
            resolvedSrcMsgId = watchers.latestSourceMsgId, resolvedExtToken = watchers.externalSourceToken,
            instantSource
        )

    var pendingBlock: PendingBlock? = null
        private set

    // From the live index, not the node config: the two agree in production, but they are one value and
    // the live index is what owns the block being filled — `blockRowCount` below seeds the gauge from it.
    private val rowsPerBlock = liveIndex.rowsPerBlock

    // Rows in the current block so far (resolve-side gauge). The boundary is cut off this rather than
    // liveIndex.isFull(), which lags (it only reflects APPLIED — consume-back — txs). Seeded from the rows
    // already applied into the open block: on a leadership change the new leader inherits a partially-filled
    // block from replay, and must cut it at the same point the old leader would have (else block sizes drift
    // across restarts — the #5817 stop/start off-by-one). Reset when a boundary is injected.
    private var rowsSinceBlock: Long = liveIndex.blockRowCount

    // A block cut is in progress: the BlockBoundary has been appended but its BlockUploaded has not yet
    // been produced. Resolution (source/ext/gc) is paused — excluded from the select — so no tx interleaves
    // between the boundary and its upload, keeping the follower's bounded pending-block buffer empty.
    private var blockInProgress: Boolean = false

    val gc = GarbageCollector(
        nodeBase, partitionStorage, partitionState, dbName, leaderTerm, replicaAppender, gcDispatcher
    )

    val srcLogProc = SourceLogProcessor(
        driver, txResolver, partitionStorage, partitionState, watchers, dbCatalog, dbName, leaderTerm,
        replicaAppender, flushTimeout, ::appendTx, ::cutBlock
    )

    // An ext-source tx that fills a block cuts it like any other, but there is no batch mid-flight to
    // stop, so the processor is handed the append alone.
    val extSrcProc =
        extSource?.let { source ->
            ExternalSourceProcessor(source, partition, blockCatalog, watchers, txResolver) { appendTx(it) }
        }

    // Records read back off the replica log, awaiting application. The partition's reader fills it
    // through [queueReplicaMessage]; its capacity is what bounds how far that reader may run ahead.
    private val replicaMsgs = Channel<Log.Record<ReplicaMessage>>(capacity = 128)


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

                gc.signal()

                blockInProgress = false
                srcLogProc.blockUploaded()
            }

            // Our own BlockUploaded, read back after uploadBlock already rolled the index — nothing to do
            // but advance the watermark.
            is ReplicaMessage.BlockUploaded -> watchers.notifyApplied(record.msgId, msg.latestProcessedMsgId)

            is ReplicaMessage.NoOp -> watchers.notifyApplied(record.msgId, msg.srcMsgId)

            // Catalog already updated on the resolve side (see GarbageCollector.handleTask); nothing to do.
            is ReplicaMessage.TriesDeleted -> watchers.notifyApplied(record.msgId)
        }
    }

    // ---- resolution ----

    // Cut a block: inject the boundary (in resolution order, so it lands after this block's txs and before
    // the next block's) and pause resolution until it is read back and uploaded. Reset the row gauge.
    private suspend fun cutBlock(latestProcessedMsgId: MessageId) {
        val boundary = BlockBoundary(
            (blockCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, txResolver.resolvedExtToken,
            termId = leaderTerm
        )
        replicaAppender.append(ControlItem(boundary))
        blockInProgress = true
        rowsSinceBlock = 0
    }

    // Hand a freshly-resolved tx to the append pump, cutting a block if this tx filled one — which the
    // caller is told about, because a source batch mid-flight has to stop where that happens.
    private suspend fun appendTx(resolvedTx: ResolvedTx): Boolean {
        rowsSinceBlock += resolvedTx.allTables.sumOf { it.relation.rowCount.toLong() }
        replicaAppender.append(TxItem(resolvedTx, leaderTerm))

        if (rowsSinceBlock < rowsPerBlock) return false

        cutBlock(txResolver.resolvedSrcMsgId)
        return true
    }

    // ---- work loop ----

    override fun SelectBuilder<Unit>.selectWork() {
        // Applying is where a supersession fails the term; let it propagate (interrupts too).
        replicaMsgs.onReceive { rec ->
            try {
                applyRecord(rec)
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
        }

        if (!blockInProgress) {
            with(srcLogProc) { selectWork() }

            extSrcProc?.onTask { task ->
                watchers.runTaskGuarded(task.onComplete, extResult = task.msg.pending) {
                    extSrcProc.handleTask(task)
                }
            }

            gc.gcCh.onReceive { task ->
                watchers.runTaskGuarded(task.onComplete) { gc.handleTask(task) }
            }
        }
    }

    // Completed by [workFailed] with the cause. Write-once, and the only thing the term learns about the
    // work it no longer runs.
    private val termFailure = CompletableDeferred<Throwable>()

    /**
     * End this term, because work it no longer runs has failed with [cause].
     *
     * The logging, the watchers and the sweep of everything staged are still the term's, and so is the
     * distinction they turn on — a supersession is a clean resignation where an append fault is not — so
     * the cause has to come back here to reach them.
     */
    override fun workFailed(cause: Throwable) {
        termFailure.complete(cause)
    }

    /**
     * Wait for this term to end, then fail everything staged on it.
     *
     * Cancelling the caller is what ends the term otherwise.
     */
    suspend fun runTerm() {
        var cause: Throwable? = null
        try {
            throw termFailure.await()
        } catch (_: CancellationException) {
            // term cancellation
        } catch (e: LeaderSupersededException) {
            // superseded by a newer leader — expected, not a query-facing fault; the transport
            // re-follows on the next rebalance. Don't poison the watchers.
            LOG.info("[$dbName] ${e.message}")
            cause = e
        } catch (t: Throwable) {
            // A genuine term fault (e.g. an append fault) surfaces to queries as a failed term.
            // Idempotent — the apply arm may already have notified for its own faults.
            LOG.error(t) { "[$dbName] leader term failed" }
            cause = t
            watchers.notifyError(t)
        } finally {
            val pendingCause = cause ?: CancellationException("leader term closed")

            // Nothing may be left awaiting the term once it has gone: whatever is staged, paused, or
            // still queued gets failed here. Each task's own `abandon` picks the failure *kind*, so this
            // is a flat sweep with no per-caller special-casing.
            //
            // Miss anything and the symptom is a hang, not an error — and for a source-log batch that
            // hang is on the transport's poll thread (inside `processRecords`), which is also the sole
            // servicer of the transport's unregister. So it wedges the whole subscription teardown and
            // blows DatabaseCatalog.close's bound (#5711 / #5817).
            txResolver.failPending(pendingCause)
            srcLogProc.shutdown(pendingCause)
            extSrcProc?.shutdown(pendingCause)
            gc.shutdown(pendingCause)

            replicaAppender.shutdown(pendingCause)

            // The partition's reader is suspended on a send here rather than awaiting a task, so a
            // close is all it needs — as a cancellation, since it is the reader's own coroutine that
            // sees it and a benign teardown must not poison the watchers.
            replicaMsgs.close(pendingCause.asCancellation())
        }
    }

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) =
        srcLogProc.processRecords(records)

    override fun close() {
        extSrcProc?.close()
        driver.close()
        // Frees every resolved-but-not-applied tx — safe only once the term's job has been joined, so the
        // persister and the pumps are gone.
        txResolver.close()
    }
}
