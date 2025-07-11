package xtdb.indexer

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey
import xtdb.table.TableRef
import xtdb.trie.BlockIndex

interface LiveIndex : Watermark.Source, AutoCloseable {

    interface Watermark : AutoCloseable {
        val allColumnFields: Map<TableRef, Map<String, Field>>
        fun liveTable(table: TableRef): LiveTable.Watermark
        val liveTables: Iterable<TableRef>
    }

    interface Tx : AutoCloseable {
        fun liveTable(table: TableRef): LiveTable.Tx
        fun openWatermark(): Watermark

        fun commit()
        fun abort()
    }

    val latestCompletedTx: TransactionKey?

    fun liveTable(table: TableRef): LiveTable
    val liveTables: Iterable<TableRef>

    // N.B. LiveIndex.Watermark and xtdb.indexer.Watermark are different classes
    // there used to be quite a lot of difference between them
    // now - not so much, they could probably be combined
    override fun openWatermark(): xtdb.indexer.Watermark

    fun startTx(txKey: TransactionKey): Tx

    fun isFull(): Boolean
    fun finishBlock(blockIdx: BlockIndex) : Collection<TableRef>
    fun nextBlock()
}
