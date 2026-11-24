package xtdb.table

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName
import xtdb.util.asIid
import xtdb.util.closeOnCatch

/** The columns whose values identify a row of a table, in key order. */
data class PrimaryKey(val columns: List<ColumnName>) {
    init {
        require(columns.isNotEmpty()) { "a primary key names at least one column" }
    }

    private fun compositeUnsupported() = Unsupported(
        "multi-column primary keys are not yet supported", "xtdb/composite-primary-key", mapOf("columns" to columns)
    )

    /** The key's single column; a multi-column key is [Unsupported]. */
    val column: ColumnName
        get() = columns.singleOrNull() ?: throw compositeUnsupported()

    private fun iidOf(keyValues: List<Any>): ByteArray =
        (keyValues.singleOrNull() ?: throw compositeUnsupported()).asIid

    /**
     * The IID of the row [doc] describes, from its key columns' values.
     * Throws `xtdb.tx/missing-id` unless [doc] has a value in every key column,
     * `xtdb/invalid-id` for a value of a type that can't key a row.
     */
    fun iid(doc: Map<String, *>): ByteArray =
        iidOf(columns.map { col -> doc[col] ?: throw Incorrect("missing $col", "xtdb.tx/missing-id", mapOf("doc" to doc)) })

    private fun keyVectors(rel: RelationReader): List<VectorReader> =
        columns.map { col ->
            val keyVec = rel.vectorForOrNull(col)
            if (keyVec == null || (0 until rel.rowCount).any(keyVec::isNull))
                throw Incorrect("missing '$col'", "xtdb.tx/missing-id", mapOf("column" to col))
            keyVec
        }

    private fun List<VectorReader>.iidAt(idx: Int): ByteArray = iidOf(map { it.getObject(idx)!! })

    /**
     * The IIDs of [rel]'s rows, from its key columns, as an `_iid` vector for the caller to close.
     * Throws as [requireKeys] does.
     */
    fun openIids(allocator: BufferAllocator, rel: RelationReader): FixedSizeBinaryVector {
        val keyVecs = keyVectors(rel)
        return FixedSizeBinaryVector(allocator, "_iid", false, 16).closeOnCatch { iids ->
            repeat(rel.rowCount) { iids.writeBytes(keyVecs.iidAt(it)) }
            iids
        }
    }

    /**
     * Throws as indexing [rel] would: `xtdb.tx/missing-id` unless every row has a value in every key column,
     * `xtdb/invalid-id` for a value of a type that can't key a row.
     */
    fun requireKeys(rel: RelationReader) {
        val keyVecs = keyVectors(rel)
        repeat(rel.rowCount) { keyVecs.iidAt(it) }
    }

    companion object {
        /** What a table is keyed on unless it declares otherwise. */
        @JvmField
        val ID = PrimaryKey(listOf("_id"))
    }
}
