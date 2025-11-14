package xtdb.indexer

import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.arrow.VectorReader
import xtdb.database.Database
import xtdb.indexer.LiveIndex
import java.time.Instant
import java.time.ZoneId

interface Indexer : AutoCloseable {

    interface ForDatabase : AutoCloseable {
        fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?
        ): TransactionResult

        fun addTxRow(txKey: TransactionKey, error: Throwable?)
    }

    interface TxSink {
        fun onCommit(txKey: TransactionKey, liveIdxTx: LiveIndex.Tx)
    }

    fun openForDatabase(db: Database, txSink: TxSink?): ForDatabase
}
