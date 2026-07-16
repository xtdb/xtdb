package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.storage.Storage
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.storage.ReadOnlyBufferPool
import xtdb.api.DatabaseName
import xtdb.util.closeAll
import xtdb.util.safelyOpening

class DatabaseStorage(
    val sourceLogOrNull: Log<SourceMessage>?,
    val replicaLogOrNull: Log<ReplicaMessage>?,
    val bufferPoolOrNull: BufferPool?,
    val metadataManagerOrNull: PageMetadata.Factory?,
) : AutoCloseable {
    val sourceLog: Log<SourceMessage> get() = sourceLogOrNull ?: error("no source-log")
    val replicaLog: Log<ReplicaMessage> get() = replicaLogOrNull ?: error("no replica-log")
    val bufferPool: BufferPool get() = bufferPoolOrNull ?: error("no buffer-pool")
    val metadataManager: PageMetadata.Factory get() = metadataManagerOrNull ?: error("no metadata-manager")

    override fun close() {
        listOf(metadataManagerOrNull, bufferPoolOrNull, replicaLogOrNull, sourceLogOrNull).closeAll()
    }

    companion object {
        @JvmStatic
        fun open(
            allocator: BufferAllocator,
            base: NodeBase,
            dbName: DatabaseName,
            dbConfig: Database.Config,
        ): DatabaseStorage = safelyOpening {
            val readOnly = dbConfig.isReadOnly
            val remotes = base.remotes

            val bufferPool = open {
                val bp = dbConfig.storage.open(
                    allocator, base.memoryCache, base.diskCache,
                    dbName, base.meterRegistry, Storage.VERSION,
                    base.remotes,
                )
                if (readOnly) ReadOnlyBufferPool(bp) else bp
            }

            val metadataManager = open { PageMetadata.factory(allocator, bufferPool) }

            val sourceLog = open {
                if (readOnly) dbConfig.log.openReadOnlySourceLog(remotes, dbConfig.partitions)
                else dbConfig.log.openSourceLog(remotes, dbConfig.partitions)
            }

            val replicaLog = open {
                if (readOnly) dbConfig.log.openReadOnlyReplicaLog(remotes, dbConfig.partitions)
                else dbConfig.log.openReplicaLog(remotes, dbConfig.partitions)
            }

            DatabaseStorage(sourceLog, replicaLog, bufferPool, metadataManager)
        }
    }
}
