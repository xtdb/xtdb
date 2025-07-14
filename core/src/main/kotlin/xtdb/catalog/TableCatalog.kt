package xtdb.catalog

import org.apache.arrow.vector.types.pojo.Field
import xtdb.table.TableRef
import xtdb.trie.ColumnName

interface TableCatalog {
    fun rowCount(table: TableRef): Long?
    fun getField(table: TableRef, columnName: ColumnName): Field?
    fun getFields(table: TableRef): Map<ColumnName, Field>?
    val fields: Map<TableRef, Map<ColumnName, Field>>
}