package xtdb.indexer

import org.apache.arrow.vector.VectorSchemaRoot
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.Log
import java.time.Duration
import java.time.Instant

interface IIndexer {
    fun indexTx(txId: Long, msgTimestamp: Instant, txRoot: VectorSchemaRoot?): TransactionResult
    fun latestCompletedTx(): TransactionKey?
    fun latestCompletedChunkTx(): TransactionKey?

    /**
     * May return a TransactionResult if available.
     */
    fun awaitTx(txId: Long, timeout: Duration): TransactionKey
    fun forceFlush(record: Log.Record)

    fun indexerError(): Throwable
}
