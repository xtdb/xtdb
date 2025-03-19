package xtdb.indexer

import org.apache.arrow.vector.VectorSchemaRoot
import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import java.time.Instant

interface IIndexer {
    fun indexTx(msgId: MessageId, msgTimestamp: Instant, txRoot: VectorSchemaRoot?): TransactionResult
}
