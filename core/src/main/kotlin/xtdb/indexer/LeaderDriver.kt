package xtdb.indexer

import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.arrow.RelationReader
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.types.MessageId

/**
 * The leader term's log appends and live-index writes, behind one seam — so a test can fail or stall one,
 * which the real in-memory logs never do.
 *
 * Deliberately narrow. In-memory state mutations that happen to sit on the leader's path —
 * `trieCatalog`, `dbCatalog`, `watchers`, the GC signals — stay on the processor, as do reads of
 * in-memory state (`liveIndex.isFull()`, `tableCatalog.currentBlockIndex`). A wrapper holds real
 * state objects, so those reads stay consistent with what the driver has applied.
 *
 * The block upload is deliberately not here either: it is [BlockCutter]'s, through [BlockUploader], and it
 * reaches both logs and object storage of its own accord.
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

    /** Ask the source log to cut a block, on the flush-timeout path. Returns the message's position. */
    suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId
}

internal class RealLeaderDriver(
    partitionStorage: PartitionStorage,
    partitionState: PartitionState,
) : LeaderDriver {

    private val sourceLog = partitionStorage.sourceLog
    private val replicaLog = partitionStorage.replicaLog
    private val liveIndex = partitionState.liveIndex

    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
        replicaLog.appendMessage(msg)

    override suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) =
        liveIndex.commitTx(txKey, tables)

    override suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId =
        sourceLog.appendMessage(SourceMessage.FlushBlock(expectedBlockIdx)).msgId
}
