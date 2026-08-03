package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectClause1
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.SourceMessage
import xtdb.arrow.RelationReader
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId

/**
 * Re-cast a term-teardown cause as a cancellation, preserving the original for the logs.
 *
 * The failure *kind* is load-bearing for anything the transport's poll thread observes: a
 * CancellationException unwinds `processRecords` as cancellation, while anything else reaches the Database
 * scope's `CoroutineExceptionHandler`, which calls `watchers.notifyError`.
 */
private fun Throwable?.asCancellation(): CancellationException =
    this as? CancellationException
        ?: CancellationException("leader term closed").also { c -> this?.let { c.initCause(it) } }

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
 * One owned object rather than three members on [LeaderDriver]: submitting, selecting and shutting
 * down are the three ends of a single pipe, and keeping them together is what stops them drifting.
 */
internal interface SourceBatches {
    /** Armed by the persister's select, alongside its own channels. */
    val onBatch: SelectClause1<SourceBatch>

    /** Hand a batch to the persister; the returned handle completes once it has been processed. */
    suspend fun submit(records: List<Log.Record<SourceMessage>>): Deferred<Unit>

    /**
     * Term teardown: no batch will be processed after this, and a later [submit] throws.
     *
     * Fails everyone still waiting — senders via the close cause, and whatever is still queued via
     * [SourceBatch.abandon]. Miss the queued ones and the symptom is a hang on the transport's poll
     * thread, which is also the sole servicer of the transport's unregister, so it wedges the whole
     * subscription teardown (#5711 / #5817).
     */
    fun shutdown(cause: Throwable?)
}

/**
 * The leader term's observable external effects, behind one seam.
 *
 * The [LeaderLogProcessor] keeps the concurrency that drives these — the persister select loop, the
 * background append, the staging resolver — and reaches the outside world only through here. That
 * makes the leader simulable: a mock driver can stall an upload, fail an append, or feed back a
 * record from a newer term, none of which the real logs express in memory.
 *
 * Deliberately narrow. In-memory state mutations that happen to sit on the leader's path —
 * `trieCatalog`, `dbCatalog`, `watchers`, the GC signals — stay on the processor, as do reads of
 * in-memory state (`liveIndex.isFull()`, `blockCatalog.currentBlockIndex`). A mock holds real state
 * objects, so those reads stay consistent with what the driver has applied.
 *
 * The replica-log tail ([tailReplica]) is here despite being a read: since #5817 it is how the leader
 * learns its own writes landed, and where the term fence bites, so a sim has to be able to feed it a
 * superseding record that the real in-memory log would never produce on its own.
 */
internal interface LeaderDriver : AutoCloseable {

    val sourceBatches: SourceBatches

    /**
     * Append [msg] to the replica log and await its position.
     *
     * A plain append: nothing here is atomic across messages, and nothing needs to be. A superseded
     * leader is fenced by the term its records carry, checked when it reads them back (#5817) — not by
     * an append that fails.
     */
    suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata

    /**
     * Tail our own replica log from [afterMsgId], handing each batch of records back for application.
     * Suspends until cancelled — a plain tail, independent of the source-log group subscription that
     * drives leader election.
     */
    suspend fun tailReplica(afterMsgId: MessageId, process: suspend (List<Log.Record<ReplicaMessage>>) -> Unit)

    /** Commit a resolved tx's writes into the durable live index. */
    suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>)

    /**
     * Snapshot the live index into block files, append the [BlockBoundary]'s matching `BlockUploaded`,
     * and roll the index. Returns the `BlockUploaded`'s replica-log position.
     *
     * [termId] is the *appending* term, which is not always [boundary]'s: a transition finishes the
     * previous leader's pending block, and the `BlockUploaded` must carry the new term or followers
     * that have already advanced would fence it and never complete the block.
     */
    suspend fun uploadBlock(boundaryMsgId: MessageId, termId: Long, boundary: BlockBoundary): MessageId

    /** Ask the source log to cut a block, on the flush-timeout path. Returns the message's position. */
    suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId
}

internal class RealLeaderDriver(
    partitionStorage: PartitionStorage,
    partitionState: PartitionState,
    private val blockUploader: BlockUploader,
) : LeaderDriver {

    private val sourceLog = partitionStorage.sourceLog
    private val replicaLog = partitionStorage.replicaLog
    private val liveIndex = partitionState.liveIndex

    // capacity 1: the poll thread can deposit one batch ahead and read the next while the persister
    // works, bounding lookahead to ~2 batches. Backpressure falls out of a full channel suspending
    // the send.
    override val sourceBatches = object : SourceBatches {
        private val ch = Channel<SourceBatch>(capacity = 1, onUndeliveredElement = { it.abandon(null) })

        override val onBatch get() = ch.onReceive

        override suspend fun submit(records: List<Log.Record<SourceMessage>>): Deferred<Unit> =
            SourceBatch(records).also { ch.send(it) }.onComplete

        // Close and drain, in that order: `close` alone doesn't visit buffered elements (only `cancel`
        // does), so a queued batch's submitter would wait forever; and closing first means no send can
        // slip into a buffer we've already drained. The close cause must be a cancellation too — it is
        // what a later `send` throws, and the poll thread sends here. Only safe on the persister's exit
        // path: it is the sole receiver, so nothing competes with these `tryReceive`s.
        override fun shutdown(cause: Throwable?) {
            ch.close(cause.asCancellation())
            while (true) (ch.tryReceive().getOrNull() ?: break).abandon(cause)
        }
    }

    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
        replicaLog.appendMessage(msg)

    override suspend fun tailReplica(
        afterMsgId: MessageId, process: suspend (List<Log.Record<ReplicaMessage>>) -> Unit,
    ) = replicaLog.tailAll(afterMsgId) { records -> process(records) }

    override suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) =
        liveIndex.commitTx(txKey, tables)

    override suspend fun uploadBlock(boundaryMsgId: MessageId, termId: Long, boundary: BlockBoundary): MessageId =
        blockUploader.uploadBlock(boundaryMsgId, termId, boundary)

    override suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId =
        sourceLog.appendMessage(SourceMessage.FlushBlock(expectedBlockIdx)).msgId

    // Nothing to release: the logs outlive the term, and the only driver-owned resource is
    // `sourceBatches`' channel, which the term shuts down (with its cause) via `sourceBatches.shutdown`.
    override fun close() = Unit
}
