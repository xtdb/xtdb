package xtdb.indexer

import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.arrow.VectorReader
import xtdb.database.Database
import java.time.Instant
import java.time.ZoneId

interface Indexer : AutoCloseable {

    interface ForDatabase : AutoCloseable {
        fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?
        ): TransactionResult
    }

    fun openForDatabase(db: Database): ForDatabase
}
