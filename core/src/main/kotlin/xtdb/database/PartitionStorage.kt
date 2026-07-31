package xtdb.database

import xtdb.api.log.PartitionLog
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.util.closeAll

/**
 * A single [partition]'s view of the database's storage: the shared source/replica logs, each bound to
 * this partition as a [PartitionLog], plus the partition's own [BufferPool] and metadata manager.
 *
 * Closing frees only the per-partition state — the logs are owned by the [Database] and outlive every
 * partition.
 */
class PartitionStorage(
    val logs: DatabaseLogs,
    val bufferPoolOrNull: BufferPool?,
    val metadataManagerOrNull: PageMetadata.Factory?,
    // The partition this view serves; drives the PartitionLog binding below. Defaulted to 0 because
    // single-partition is the only shape reachable until the multi-partition gate lifts (#5837).
    val partition: Int = 0,
) : AutoCloseable {
    val sourceLog: PartitionLog<SourceMessage> get() = PartitionLog(logs.sourceLog, partition)
    val replicaLog: PartitionLog<ReplicaMessage> get() = PartitionLog(logs.replicaLog, partition)
    val bufferPool: BufferPool get() = bufferPoolOrNull ?: error("no buffer-pool")
    val metadataManager: PageMetadata.Factory get() = metadataManagerOrNull ?: error("no metadata-manager")

    override fun close() {
        listOf(metadataManagerOrNull, bufferPoolOrNull).closeAll()
    }
}
