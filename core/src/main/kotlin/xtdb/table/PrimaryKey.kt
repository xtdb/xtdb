package xtdb.table

import xtdb.api.error.Unsupported
import xtdb.trie.ColumnName

/** The columns whose values identify a row of a table, in key order. */
data class PrimaryKey(val columns: List<ColumnName>) {
    init {
        require(columns.isNotEmpty()) { "a primary key names at least one column" }
    }

    /** The key's single column; a multi-column key is [Unsupported]. */
    val column: ColumnName
        get() = columns.singleOrNull() ?: throw Unsupported(
            "multi-column primary keys are not yet supported", "xtdb/composite-primary-key", mapOf("columns" to columns)
        )

    companion object {
        /** What a table is keyed on unless it declares otherwise. */
        val ID = PrimaryKey(listOf("_id"))
    }
}
