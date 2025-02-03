package xtdb.indexer

import org.apache.arrow.vector.VectorSchemaRoot
import xtdb.api.TransactionResult
import xtdb.api.log.Log
import xtdb.api.log.LogOffset
import java.time.Instant

interface IIndexer {
    fun indexTx(msgOffset: LogOffset, msgTimestamp: Instant, txRoot: VectorSchemaRoot?): TransactionResult
}
