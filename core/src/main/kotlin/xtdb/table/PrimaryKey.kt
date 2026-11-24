package xtdb.table

import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
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

    /** [rel]'s key column; `xtdb.tx/missing-id` unless every row has a key value. */
    fun keyVector(rel: RelationReader): VectorReader {
        val keyVec = rel.vectorForOrNull(column)
        if (keyVec == null || (0 until rel.rowCount).any(keyVec::isNull))
            throw Incorrect("missing '$column'", "xtdb.tx/missing-id", mapOf("column" to column))
        return keyVec
    }

    companion object {
        /** What a table is keyed on unless it declares otherwise. */
        @JvmField
        val ID = PrimaryKey(listOf("_id"))
    }
}
