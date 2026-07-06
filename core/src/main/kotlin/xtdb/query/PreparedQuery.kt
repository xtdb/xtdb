package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.ResultCursor
import xtdb.trie.ColumnName
import xtdb.arrow.RelationReader

interface PreparedQuery {
    val paramCount: Int
    val columnNames: List<ColumnName>
    fun getColumnFields(paramFields: List<Field>): List<Field>

    val warnings: List<String>

    // the statement this was prepared from, when there is one (null for embedded RA / `xt/q`). Lets the
    // connection gate a read on its statement kind — e.g. SHOW bypasses the access-mode gate.
    val parsed: ParsedStatement? get() = null

    fun openQuery(args: RelationReader?, opts: QueryOpts): ResultCursor
}
