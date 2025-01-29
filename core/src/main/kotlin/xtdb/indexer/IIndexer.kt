package xtdb.indexer

import org.apache.arrow.vector.VectorSchemaRoot
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.Log
import java.time.Instant

interface IIndexer {
    fun indexTx(txId: Long, msgTimestamp: Instant, txRoot: VectorSchemaRoot): TransactionResult
    fun latestCompletedTx(): TransactionKey?
    fun latestCompletedChunkTx(): TransactionKey?

    fun forceFlush(record: Log.Record)
    fun indexerError(): Throwable
}