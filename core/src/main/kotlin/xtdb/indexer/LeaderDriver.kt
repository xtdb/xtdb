package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectClause1
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.SourceMessage
import xtdb.arrow.RelationReader
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId

/** One poll batch inbound from the transport, plus the handle its submitter awaits. */
internal class SourceBatch(val records: List<Log.Record<SourceMessage>>) {
    val onComplete = CompletableDeferred<Unit>()
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

    /** Term teardown: no batch will be processed after this, and a later [submit] throws [cause]. */
    fun shutdown(cause: Throwable?)
}

/**
 * The leader term's observable external effects, behind one seam.
 *
 * The [LeaderLogProcessor] keeps the concurrency that drives these — the persister select loop, the
 * background append, the staging resolver — and reaches the outside world only through here. That
 * makes the leader simulable: a mock driver can model transactional-producer fencing, stall an
 * upload, or fail an append, none of which the real logs express in memory.
 *
 * Deliberately narrow. In-memory state mutations that happen to sit on the leader's path —
 * `trieCatalog`, `dbCatalog`, `watchers`, the GC signals — stay on the processor, as do all *reads*
 * (`liveIndex.isFull()`, `blockCatalog.currentBlockIndex`). A mock holds real state objects, so
 * those reads stay consistent with what the driver has applied.
 */
internal interface LeaderDriver : AutoCloseable {

    val sourceBatches: SourceBatches

    /**
     * Append [msgs] to the replica log as one atomic unit, in order, and await their positions.
     *
     * [msgs] is consumed *inside* the append's atomic unit, so a caller may serialize each message
     * lazily rather than materialising a whole batch's worth of Arrow bytes up front — a sealed
     * batch can run to a full block (`rowsPerBlock`, 100k rows by default).
     */
    suspend fun appendToReplica(msgs: Sequence<ReplicaMessage>): List<Log.MessageMetadata>

    /** Commit a resolved tx's writes into the durable live index. */
    suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>)

    /**
     * Snapshot the live index into block files, append the [BlockBoundary]'s matching `BlockUploaded`,
     * and roll the index. Returns the `BlockUploaded`'s replica-log position.
     */
    suspend fun uploadBlock(boundaryMsgId: MessageId, boundary: BlockBoundary): MessageId

    /** Ask the source log to cut a block, on the flush-timeout path. Returns the message's position. */
    suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId
}

/**
 * Owns the leader term's replica producer — the only place besides `LogProcessor.openLeader` that
 * names one. The producer is term-scoped and opened by the transition (which needs it for its own
 * replay-target probe), so it arrives already open and is closed here when the term ends.
 */
internal class RealLeaderDriver(
    private val replicaProducer: Log.AtomicProducer<ReplicaMessage>,
    partitionStorage: PartitionStorage,
    partitionState: PartitionState,
    private val blockUploader: BlockUploader,
) : LeaderDriver {

    private val sourceLog = partitionStorage.sourceLog
    private val liveIndex = partitionState.liveIndex

    // capacity 1: the poll thread can deposit one batch ahead and read the next while the persister
    // works, bounding lookahead to ~2 batches. Backpressure falls out of a full channel suspending
    // the send.
    override val sourceBatches = object : SourceBatches {
        private val ch = Channel<SourceBatch>(capacity = 1, onUndeliveredElement = { it.onComplete.cancel() })

        override val onBatch get() = ch.onReceive

        override suspend fun submit(records: List<Log.Record<SourceMessage>>): Deferred<Unit> =
            SourceBatch(records).also { ch.send(it) }.onComplete

        override fun shutdown(cause: Throwable?) {
            ch.close(cause)
        }
    }

    override suspend fun appendToReplica(msgs: Sequence<ReplicaMessage>): List<Log.MessageMetadata> =
        replicaProducer.withTx { tx -> msgs.map { tx.appendMessage(it) }.toList() }.map { it.await() }

    override suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) =
        liveIndex.commitTx(txKey, tables)

    override suspend fun uploadBlock(boundaryMsgId: MessageId, boundary: BlockBoundary): MessageId =
        blockUploader.uploadBlock(replicaProducer, boundaryMsgId, boundary)

    override suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId =
        sourceLog.appendMessage(SourceMessage.FlushBlock(expectedBlockIdx)).msgId

    override fun close() = replicaProducer.close()
}
