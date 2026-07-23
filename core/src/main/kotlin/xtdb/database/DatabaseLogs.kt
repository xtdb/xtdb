package xtdb.database

import xtdb.NodeBase
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.util.closeAll
import xtdb.util.safelyOpening

/**
 * The database's source/replica log pair — shared by every [DatabasePartition]
 * (a single [Log] serves all partitions), so owned and closed at the [Database] level,
 * after the partitions that consume it.
 */
class DatabaseLogs(
    val sourceLogOrNull: Log<SourceMessage>?,
    val replicaLogOrNull: Log<ReplicaMessage>?,
) : AutoCloseable {
    val sourceLog: Log<SourceMessage> get() = sourceLogOrNull ?: error("no source-log")
    val replicaLog: Log<ReplicaMessage> get() = replicaLogOrNull ?: error("no replica-log")

    override fun close() {
        listOf(replicaLogOrNull, sourceLogOrNull).closeAll()
    }

    companion object {
        @JvmStatic
        fun open(base: NodeBase, dbConfig: Database.Config): DatabaseLogs = safelyOpening {
            val readOnly = dbConfig.isReadOnly
            val remotes = base.remotes

            val sourceLog = open {
                if (readOnly) dbConfig.log.openReadOnlySourceLog(remotes, dbConfig.partitions)
                else dbConfig.log.openSourceLog(remotes, dbConfig.partitions)
            }

            val replicaLog = open {
                if (readOnly) dbConfig.log.openReadOnlyReplicaLog(remotes, dbConfig.partitions)
                else dbConfig.log.openReplicaLog(remotes, dbConfig.partitions)
            }

            DatabaseLogs(sourceLog, replicaLog)
        }
    }
}
