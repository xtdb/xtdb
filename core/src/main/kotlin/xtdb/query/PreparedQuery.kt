package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.IResultCursor
import xtdb.trie.ColumnName
import xtdb.arrow.RelationReader

interface PreparedQuery {
    val paramCount: Int
    val columnNames: List<ColumnName>
    fun getColumnFields(paramFields: List<Field>): List<Field>

    val warnings: List<String>

    fun openQuery(opts: Any?): IResultCursor<RelationReader>
}
