package xtdb.indexer

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey

interface LiveIndex : AutoCloseable {

    interface Watermark : AutoCloseable {
        val allColumnFields: Map<String, Map<String, Field>>
        fun liveTable(tableName: String): LiveTable.Watermark
    }

    interface Tx : AutoCloseable {
        fun liveTable(name: String): LiveTable.Tx
        fun openWatermark(): Watermark

        fun commit()
        fun abort()
    }

    val latestCompletedTx: TransactionKey?
    val latestCompletedChunkTx: TransactionKey?

    fun liveTable(name: String): LiveTable

    // N.B. LiveIndex.Watermark and xtdb.indexer.Watermark are different classes
    // there used to be quite a lot of difference between them
    // now - not so much, they could probably be combined
    fun openWatermark(): xtdb.indexer.Watermark

    fun startTx(txKey: TransactionKey): Tx

    fun finishChunk()
    fun forceFlush(txKey: TransactionKey, expectedTxId: Long)
}