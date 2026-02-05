package xtdb.catalog

import xtdb.arrow.VectorType
import xtdb.block.proto.Partition
import xtdb.block.proto.TableBlock
import xtdb.log.proto.TrieMetadata
import xtdb.table.TableRef
import xtdb.trie.BlockIndex
import xtdb.trie.ColumnName
import java.nio.ByteBuffer
import java.nio.file.Path

data class TableBlockMetadata(
    val vecTypes: Map<ColumnName, VectorType>,
    val rowCount: Int,
    val trieKey: String,
    val dataFileSize: Long,
    val trieMetadata: TrieMetadata,
    val hlls: Map<ColumnName, ByteBuffer>
)

interface TableCatalog {
    fun rowCount(table: TableRef): Long?

    fun getType(table: TableRef, columnName: ColumnName): VectorType?
    fun getTypes(table: TableRef): Map<ColumnName, VectorType>?
    val types: Map<TableRef, Map<ColumnName, VectorType>>

    fun refresh()

    fun finishBlock(
        tableMetadata: Map<TableRef, TableBlockMetadata>,
        tablePartitions: Map<TableRef, List<Partition>>
    ): Map<TableRef, TableBlock>
}
