package xtdb.indexer

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
 * The leader term's observable external effects, behind one seam.
 *
 * These are driven from the log processor's work loop, and reach the outside world only through
 * here. That makes a leader simulable: a mock driver can stall an upload or fail an append, neither
 * of which the real logs express in memory.
 *
 * Deliberately narrow. In-memory state mutations that happen to sit on the leader's path —
 * `trieCatalog`, `dbCatalog`, `watchers`, the GC signals — stay on the processor, as do reads of
 * in-memory state (`liveIndex.isFull()`, `tableCatalog.currentBlockIndex`). A mock holds real state
 * objects, so those reads stay consistent with what the driver has applied.
 */
internal interface LeaderDriver {

    /**
     * Append [msg] to the replica log and await its position.
     *
     * A plain append: nothing here is atomic across messages, and nothing needs to be. A superseded
     * leader is fenced by the term its records carry, checked when it reads them back (#5817) — not by
     * an append that fails.
     */
    suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata

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

    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
        replicaLog.appendMessage(msg)

    override suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) =
        liveIndex.commitTx(txKey, tables)

    override suspend fun uploadBlock(boundaryMsgId: MessageId, termId: Long, boundary: BlockBoundary): MessageId =
        blockUploader.uploadBlock(boundaryMsgId, termId, boundary)

    override suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId =
        sourceLog.appendMessage(SourceMessage.FlushBlock(expectedBlockIdx)).msgId
}
