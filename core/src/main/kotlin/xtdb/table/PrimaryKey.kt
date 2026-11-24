package xtdb.table

import xtdb.trie.ColumnName

/** The columns whose values identify a row of a table, in key order. */
data class PrimaryKey(val columns: List<ColumnName>) {
    init {
        require(columns.isNotEmpty()) { "a primary key names at least one column" }
    }

    companion object {
        /** What a table is keyed on unless it declares otherwise. */
        val ID = PrimaryKey(listOf("_id"))
    }
}
