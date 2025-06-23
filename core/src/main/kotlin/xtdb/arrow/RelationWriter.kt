package xtdb.arrow

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.toFieldType
import xtdb.util.normalForm

interface RelationWriter : RelationReader {

    override val vectors: Collection<VectorWriter>
    override fun vectorForOrNull(name: String): VectorWriter?
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    fun vectorFor(name: String, fieldType: FieldType): VectorWriter = unsupported("vectorFor/2")
    override fun get(name: String) = vectorFor(name)

    fun endRow(): Int

    fun rowCopier(rel: RelationReader): RowCopier {
        val copiers = rel.vectors.map { it.rowCopier(vectorForOrNull(it.name) ?: error("missing ${it.name} vector")) }

        return RowCopier { srcIdx ->
            copiers.forEach { it.copyRow(srcIdx) }
            endRow()
        }
    }

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
            val normalKey = when (k) {
                is String -> k
                is Symbol -> normalForm(k).toString()
                is Keyword -> normalForm(k.sym).toString()
                else -> throw IllegalArgumentException("Column name must be a string, keyword or symbol")
            }

            val vector = vectorForOrNull(normalKey) ?: vectorFor(normalKey, v.toFieldType())

            try {
                vector.writeObject(v)
            } catch (_: InvalidWriteObjectException) {
                vectorFor(normalKey, v.toFieldType()).writeObject(v)
            }
        }

        endRow()
    }

    fun writeRows(vararg rows: Map<*, *>) {
        rows.forEach(::writeRow)
    }

    val asReader: RelationReader get() = this
}