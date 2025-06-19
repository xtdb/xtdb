package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.util.normalForm

interface RelationWriter : RelationReader {

    override val vectors: Collection<VectorWriter>
    override fun vectorForOrNull(name: String): VectorWriter?
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    fun vectorFor(name: String, fieldType: FieldType): VectorWriter = unsupported("vectorFor/2")
    override fun get(name: String) = vectorFor(name)

    fun endRow(): Int

    fun rowCopier(rel: RelationReader): RowCopier

    fun append(rel: RelationReader) {
        val copier = rowCopier(rel)
        repeat(rel.rowCount) { copier.copyRow(it) }
    }

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    fun clear()

    fun writeRow(row: Map<*, *>) {
        row.forEach { (k, v) ->
            val vector = this[when (k) {
                is String -> k
                is Keyword -> normalForm(k.sym).toString()
                else -> throw IllegalArgumentException("Column name must be a string or keyword")
            }]

            vector.writeObject(v)
        }
        endRow()
    }

    fun writeRows(vararg rows: Map<*, *>) {
        rows.forEach(::writeRow)
    }
}