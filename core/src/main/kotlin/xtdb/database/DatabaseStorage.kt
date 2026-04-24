package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.LogOffset
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.storage.Storage
import xtdb.catalog.BlockCatalog.Companion.latestBlock
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.storage.ReadOnlyBufferPool
import xtdb.table.DatabaseName
import xtdb.time.microsAsInstant
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.closeAll
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.safelyOpening

private val LOG = DatabaseStorage::class.logger

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
        private fun logNewEpoch() {
            LOG.info(
                "Starting node with a log that has a different epoch than the latest completed tx " +
                        "(this is expected if you are starting a new epoch) " +
                        "- skipping offset validation."
            )
        }

        private fun throwIllegalLogState(
            latestSubmittedOffset: LogOffset, logEpoch: Int, completedEpoch: Int, completedOffset: MessageId
        ): Nothing {
            val logState =
                if (latestSubmittedOffset == -1L) "the log is empty"
                else "epoch=$logEpoch, offset=$latestSubmittedOffset"

            error(
                buildString {
                    appendLine("Node failed to start due to an invalid transaction log state ($logState) " +
                            "that does not correspond with the latest indexed transaction " +
                            "(epoch=$completedEpoch and offset=$completedOffset).")
                    appendLine()
                    append("Please see https://docs.xtdb.com/ops/backup-and-restore/out-of-sync-log.html " +
                            "for more information and next steps.")
                }
            )
        }

        private fun validateOffsets(log: Log<SourceMessage>, latestCompletedTx: TransactionKey?) {
            if (latestCompletedTx == null) return

            val completedTxId = latestCompletedTx.txId
            val completedOffset = msgIdToOffset(completedTxId)
            val completedEpoch = msgIdToEpoch(completedTxId)
            val logEpoch = log.epoch
            val latestSubmittedOffset = log.latestSubmittedOffset

            when {
                completedEpoch != logEpoch -> logNewEpoch()

                latestSubmittedOffset < completedOffset ->
                    throwIllegalLogState(latestSubmittedOffset, logEpoch, completedEpoch, completedOffset)
            }
        }

        @JvmStatic
        fun open(
            allocator: BufferAllocator,
            base: NodeBase,
            dbName: DatabaseName,
            dbConfig: Database.Config,
        ): DatabaseStorage = safelyOpening {
            val readOnly = dbConfig.isReadOnly
            val logClusters = base.logClusters

            val bufferPool = open {
                val bp = dbConfig.storage.open(
                    allocator, base.memoryCache, base.diskCache,
                    dbName, base.meterRegistry, Storage.VERSION,
                    base.remotes,
                )
                if (readOnly) ReadOnlyBufferPool(bp) else bp
            }

            val metadataManager = open { PageMetadata.factory(allocator, bufferPool) }

            val latestCompletedTx = bufferPool.latestBlock
                ?.takeIf { it.hasLatestCompletedTx() }
                ?.latestCompletedTx
                ?.let { TransactionKey(it.txId, it.systemTime.microsAsInstant) }

            val sourceLog = open {
                val log = if (readOnly) dbConfig.log.openReadOnlySourceLog(logClusters)
                else dbConfig.log.openSourceLog(logClusters)

                validateOffsets(log, latestCompletedTx)
                log
            }

            val replicaLog = open {
                if (readOnly) dbConfig.log.openReadOnlyReplicaLog(logClusters)
                else dbConfig.log.openReplicaLog(logClusters)
            }

            DatabaseStorage(sourceLog, replicaLog, bufferPool, metadataManager)
        }
    }
}
