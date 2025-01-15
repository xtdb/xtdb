package xtdb.indexer

import xtdb.api.TransactionKey
import java.time.Instant
import java.time.Duration
import org.apache.arrow.vector.VectorSchemaRoot

interface IIndexer {
    fun indexTx(txId: Long, msgTimestamp: Instant, txRoot: VectorSchemaRoot): TransactionKey
    fun latestCompletedTx(): TransactionKey
    fun latestCompletedChunkTx(): TransactionKey
    /**
     * May return a TransactionResult if available.
     */
    fun awaitTx(txId: Long, timeout: Duration): TransactionKey
    fun forceFlush(txKey: TransactionKey, expectedLastChunkTxId: Long)
    fun indexerError(): Throwable
}