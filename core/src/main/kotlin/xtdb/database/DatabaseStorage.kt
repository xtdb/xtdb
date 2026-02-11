package xtdb.database

import xtdb.api.log.Log
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool

data class DatabaseStorage(
    val sourceLogOrNull: Log?,
    val replicaLogOrNull: Log?,
    val bufferPoolOrNull: BufferPool?,
    val metadataManagerOrNull: PageMetadata.Factory?,
) {
    val sourceLog: Log get() = sourceLogOrNull ?: error("no source-log")
    val replicaLog: Log get() = replicaLogOrNull ?: error("no replica-log")
    val bufferPool: BufferPool get() = bufferPoolOrNull ?: error("no buffer-pool")
    val metadataManager: PageMetadata.Factory get() = metadataManagerOrNull ?: error("no metadata-manager")
}
