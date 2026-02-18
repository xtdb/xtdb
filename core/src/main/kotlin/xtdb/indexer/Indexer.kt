package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.arrow.VectorReader
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.query.IQuerySource
import java.time.Instant
import java.time.ZoneId

interface Indexer : AutoCloseable {

    interface ForDatabase : AutoCloseable {
        fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?, userMetadata: Any?
        ): Log.Message.ResolvedTx

        fun addTxRow(txKey: TransactionKey, error: Throwable?): Log.Message.ResolvedTx
    }

    interface TxSource {
        fun onCommit(resolvedTx: Log.Message.ResolvedTx)
    }

    companion object {
        @JvmStatic
        fun queryCatalog(
            storage: DatabaseStorage,
            state: DatabaseState,
            snapSource: Snapshot.Source
        ): IQuerySource.QueryCatalog {
            val queryDb = object : IQuerySource.QueryDatabase {
                override val storage get() = storage
                override val queryState get() = state
                override fun openSnapshot() = snapSource.openSnapshot()
            }
            return object : IQuerySource.QueryCatalog {
                override val databaseNames: Collection<DatabaseName> get() = setOf(state.name)
                override fun databaseOrNull(dbName: DatabaseName) = queryDb.takeIf { dbName == state.name }
            }
        }
    }

    fun openForDatabase(
        allocator: BufferAllocator,
        storage: DatabaseStorage,
        state: DatabaseState,
        crashLogger: CrashLogger
    ): ForDatabase
}
