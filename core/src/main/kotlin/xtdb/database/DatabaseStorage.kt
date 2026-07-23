package xtdb.database

import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.util.closeAll

/**
 * A partition's view of the database's storage: the shared source/replica [logs]
 * plus the partition's own [BufferPool] and metadata manager.
 *
 * Closing frees only the per-partition state — the logs are owned by the [Database]
 * and outlive every partition.
 */
class DatabaseStorage(
    val logs: DatabaseLogs,
    val bufferPoolOrNull: BufferPool?,
    val metadataManagerOrNull: PageMetadata.Factory?,
) : AutoCloseable {
    val sourceLog: Log<SourceMessage> get() = logs.sourceLog
    val replicaLog: Log<ReplicaMessage> get() = logs.replicaLog
    val bufferPool: BufferPool get() = bufferPoolOrNull ?: error("no buffer-pool")
    val metadataManager: PageMetadata.Factory get() = metadataManagerOrNull ?: error("no metadata-manager")

    override fun close() {
        listOf(metadataManagerOrNull, bufferPoolOrNull).closeAll()
    }
}
