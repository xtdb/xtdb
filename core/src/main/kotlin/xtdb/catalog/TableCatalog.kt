package xtdb.catalog

import xtdb.arrow.VectorType
import xtdb.block.proto.Partition
import xtdb.block.proto.TableBlock
import xtdb.indexer.LiveTable
import xtdb.table.TableRef
import xtdb.trie.ColumnName

interface TableCatalog {
    fun rowCount(table: TableRef): Long?

    fun getType(table: TableRef, columnName: ColumnName): VectorType?
    fun getTypes(table: TableRef): Map<ColumnName, VectorType>?
    val types: Map<TableRef, Map<ColumnName, VectorType>>

    fun refresh()

    fun finishBlock(
        tableMetadata: Map<TableRef, LiveTable.FinishedBlock>,
        tablePartitions: Map<TableRef, List<Partition>>
    ): Map<TableRef, TableBlock>
}
