package xtdb.indexer

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey
import xtdb.api.log.Log.Message
import xtdb.api.log.MessageId
import java.time.Instant

interface LiveIndex : Watermark.Source, AutoCloseable {

    interface Watermark : AutoCloseable {
        val allColumnFields: Map<String, Map<String, Field>>
        fun liveTable(tableName: String): LiveTable.Watermark
        val liveTables: Iterable<String>
    }

    interface Tx : AutoCloseable {
        fun liveTable(name: String): LiveTable.Tx
        fun openWatermark(): Watermark

        fun commit()
        fun abort()
    }

    val latestCompletedTx: TransactionKey?
    val latestCompletedBlockTx: TransactionKey?
    val latestBlockIndex: Long

    fun liveTable(name: String): LiveTable
    val liveTables: Iterable<String>

    // N.B. LiveIndex.Watermark and xtdb.indexer.Watermark are different classes
    // there used to be quite a lot of difference between them
    // now - not so much, they could probably be combined
    override fun openWatermark(): xtdb.indexer.Watermark

    fun startTx(txKey: TransactionKey): Tx

    fun finishBlock()

    fun forceFlush(msg: Message.FlushBlock, msgId: MessageId, msgTimestamp: Instant)
}
