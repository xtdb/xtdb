package xtdb.indexer

import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.vector.IVectorReader
import java.time.Instant
import java.time.ZoneId

interface IIndexer {
    fun indexTx(
        msgId: MessageId, msgTimestamp: Instant, txOps: IVectorReader?,
        systemTime: Instant?, defaultTz: ZoneId?, user: String?
    ): TransactionResult
}
