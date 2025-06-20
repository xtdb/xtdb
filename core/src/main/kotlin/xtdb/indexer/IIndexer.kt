package xtdb.indexer

import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.arrow.VectorReader
import java.time.Instant
import java.time.ZoneId

interface IIndexer {
    fun indexTx(
        msgId: MessageId, msgTimestamp: Instant, txOps: VectorReader?,
        systemTime: Instant?, defaultTz: ZoneId?, user: String?
    ): TransactionResult
}
