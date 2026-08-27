package xtdb.indexer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.DatabaseName
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.Watchers
import xtdb.api.tx.ExternalSource
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId
import xtdb.util.debug
import xtdb.util.logger
import java.time.Duration
import java.time.InstantSource

private val LOG = LeaderLogProcessor::class.logger

/**
 * A higher-term record read back on our own replica log: a newer leader has superseded us. Thrown from
 * the apply loop to fail the term cleanly (not a query-facing fault, so it doesn't poison the watchers);
 * the transport re-follows on the next rebalance. See #5817.
 */
internal class LeaderSupersededException(message: String) : RuntimeException(message)

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

    extSource: ExternalSource?,
    skipTxs: Set<MessageId>,
    dbCatalog: Database.Catalog?,
    private val leaderTerm: Long = 0,
    instantSource: InstantSource = InstantSource.system(),
    flushTimeout: Duration,
    // Base for the GCs' delete fan-out; defaults to IO in prod, sims inject the seeded dispatcher.
    gcDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    init {
        require((dbCatalog != null) == (dbName == "xtdb")) {
            "dbCatalog must be provided iff database is 'xtdb'"
        }
    }

    private val partition = partitionStorage.partition
    private val liveIndex = partitionState.liveIndex

    private val tableCatalog = partitionState.tableCatalog

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

    /**
     * Where this term is in the block it is filling: `Filling` → `Cut` → `Uploading` → `Filling`.
     *
     * Resolution is armed only in [Filling], so no tx can interleave between a boundary and its upload —
     * which keeps the follower's bounded pending-block buffer empty. That is also why the row gauge is
     * [Filling]'s alone: outside it, nothing can move the gauge.
     */
    private sealed interface BlockState

    /**
     * Accumulating rows towards the cut, [rows] of them so far.
     *
     * The boundary is cut off this rather than `liveIndex.isFull()`, which lags — it reflects only APPLIED
     * (consume-back) txs. Seeded from the rows already applied into the open block, because a new leader
     * inherits a partially-filled block from replay and must cut it where the old leader would have, or
     * block sizes drift across restarts (the #5817 stop/start off-by-one).
     */
    private class Filling(val rows: Long) : BlockState

    /** The boundary is queued for append, and has not been read back yet. */
    private data object Cut : BlockState

    /** The boundary has been applied and the upload is in flight. */
    private class Uploading(val pendingBlock: PendingBlock) : BlockState

    private var blockState: BlockState = Filling(liveIndex.blockRowCount)

    /**
     * The block this term would have to hand on, were it demoted right now.
     *
     * Read from the transport's serialization point rather than from the work loop, and after this term has
     * been cancelled and closed — so it must not touch anything allocator-backed.
     */
    val pendingBlock: PendingBlock?
        get() = when (val state = blockState) {
            is Filling, Cut -> null
            is Uploading -> state.pendingBlock
        }

    // From the live index, not the node config: the two agree in production, but they are one value and the
    // live index is what owns the block being filled.
    private val rowsPerBlock = liveIndex.rowsPerBlock

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
            ExternalSourceProcessor(source, partition, tableCatalog, watchers, txResolver) { appendTx(it) }
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
    suspend fun applyRecord(record: Log.Record<ReplicaMessage>) {
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
                blockState = Uploading(PendingBlock(record.msgId, msg))
                // liveIndex now holds exactly this block's txs (by log order); snapshot, upload the files,
                // append BlockUploaded and roll the index — all inside uploadBlock.
                driver.uploadBlock(record.msgId, leaderTerm, msg)
                // Straight after the upload, so a demote landing here hands on nothing: the block is done.
                blockState = Filling(0)

                // the block's covered source position, as the follower does
                watchers.notifyApplied(record.msgId, msg.latestProcessedMsgId)

                gc.signal()

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
    // the next block's) and pause resolution until it is read back and uploaded.
    private suspend fun cutBlock(latestProcessedMsgId: MessageId) {
        val boundary = BlockBoundary(
            (tableCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, txResolver.resolvedExtToken,
            termId = leaderTerm
        )
        replicaAppender.append(ControlItem(boundary))
        blockState = Cut
    }

    // Hand a freshly-resolved tx to the append pump, cutting a block if this tx filled one — which the
    // caller is told about, because a source batch mid-flight has to stop where that happens.
    private suspend fun appendTx(resolvedTx: ResolvedTx): Boolean {
        val rows = when (val state = blockState) {
            is Filling -> state.rows + resolvedTx.allTables.sumOf { it.relation.rowCount.toLong() }
            // Only reachable from clauses this term arms in Filling alone, so getting here means the
            // arm-set and this state have come apart — and the gauge it would feed no longer exists.
            Cut, is Uploading -> error("[$dbName] tx resolved during a block cut")
        }

        replicaAppender.append(TxItem(resolvedTx, leaderTerm))

        if (rows < rowsPerBlock) {
            blockState = Filling(rows)
            return false
        }

        cutBlock(txResolver.resolvedSrcMsgId)
        return true
    }

    val onReplicaMsg get() = replicaMsgs.onReceive

    /**
     * Whether this term will take resolution work right now.
     *
     * False for the length of a block cut, so nothing interleaves between the boundary and its upload —
     * which is what keeps the follower's bounded pending-block buffer empty.
     */
    val acceptingResolution get() = blockState is Filling

    /**
     * Fail everything staged on this term, because it has ended with [cause].
     *
     * Nothing may be left awaiting the term once it has gone: whatever is staged, paused, or still queued
     * gets failed here. Each task's own `abandon` picks the failure *kind*, so this is a flat sweep with no
     * per-caller special-casing.
     *
     * Miss anything and the symptom is a hang, not an error — and for a source-log batch that hang is on
     * the transport's poll thread (inside `processRecords`), which is also the sole servicer of the
     * transport's unregister. So it wedges the whole subscription teardown and blows
     * `DatabaseCatalog.close`'s bound (#5711 / #5817).
     */
    fun shutdown(cause: Throwable) {
        txResolver.failPending(cause)
        srcLogProc.shutdown(cause)
        extSrcProc?.shutdown(cause)
        gc.shutdown(cause)

        replicaAppender.shutdown(cause)

        // The partition's reader is suspended on a send here rather than awaiting a task, so a close is
        // all it needs — as a cancellation, since it is the reader's own coroutine that sees it and a
        // benign teardown must not poison the watchers.
        replicaMsgs.close(cause.asCancellation())
    }

    override fun close() {
        // Frees every resolved-but-not-applied tx — safe only once the term's job has been joined, so the
        // persister and the pumps are gone.
        txResolver.close()
    }
}
