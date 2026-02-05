package xtdb.indexer

import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.arrow.VectorReader
import xtdb.database.Database
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
        ): TransactionResult

        fun addTxRow(txKey: TransactionKey, error: Throwable?)
    }

    interface TxSource {
        fun onCommit(txKey: TransactionKey, liveIdxTx: LiveIndex.Tx)
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
                override val state get() = state
                override fun openSnapshot() = snapSource.openSnapshot()
            }
            return object : IQuerySource.QueryCatalog {
                override val databaseNames: Collection<DatabaseName> get() = setOf(state.name)
                override fun databaseOrNull(dbName: DatabaseName) = queryDb.takeIf { dbName == state.name }
            }
        }
    }

    fun openForDatabase(db: Database, crashLogger: CrashLogger): ForDatabase
}
