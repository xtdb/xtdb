package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.trie.ColumnName

interface PreparedQuery {
    val paramCount: Int
    val columnNames: List<ColumnName>
    fun getColumnFields(paramFields: List<Field>): List<Field>

    val warnings: List<String>

    fun bind(opts: Any?): BoundQuery
}