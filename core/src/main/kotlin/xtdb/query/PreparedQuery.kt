package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.ResultCursor
import xtdb.api.query.QueryOpts
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

    // takes ownership of [args] — closed here on failure, otherwise by the returned cursor.
    // no separate `.use`/`closeOnCatch` on the args at the call site, or it'd double-free.
    fun openQuery(args: RelationReader?, opts: QueryOpts): ResultCursor
}
