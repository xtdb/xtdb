package xtdb.catalog

import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.VectorType
import xtdb.table.TableRef
import xtdb.trie.ColumnName

interface TableCatalog {
    fun rowCount(table: TableRef): Long?

    fun getType(table: TableRef, columnName: ColumnName): VectorType?
    fun getTypes(table: TableRef): Map<ColumnName, VectorType>?
    val types: Map<TableRef, Map<ColumnName, VectorType>>

    fun refresh()
}
