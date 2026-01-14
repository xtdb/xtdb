package xtdb.indexer

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey
import xtdb.arrow.VectorType
import xtdb.table.TableRef
import xtdb.trie.BlockIndex

interface LiveIndex : Snapshot.Source, AutoCloseable {

    interface Snapshot : AutoCloseable {
        val allColumnFields: Map<TableRef, Map<String, Field>>
        val allColumnTypes: Map<TableRef, Map<String, VectorType>>
        fun liveTable(table: TableRef): LiveTable.Snapshot
        val liveTables: Iterable<TableRef>
    }

    interface Tx : AutoCloseable {
        fun liveTable(table: TableRef): LiveTable.Tx
        fun openSnapshot(): Snapshot

        fun commit()
        fun abort()
    }

    val latestCompletedTx: TransactionKey?

    fun liveTable(table: TableRef): LiveTable
    val liveTables: Iterable<TableRef>

    // N.B. LiveIndex.Snapshot and xtdb.indexer.Snapshot are different classes
    // there used to be quite a lot of difference between them
    // now - not so much, they could probably be combined
    override fun openSnapshot(): xtdb.indexer.Snapshot

    fun startTx(txKey: TransactionKey): Tx

    fun isFull(): Boolean
    fun finishBlock(blockIdx: BlockIndex) : Collection<TableRef>
    fun nextBlock()
}
