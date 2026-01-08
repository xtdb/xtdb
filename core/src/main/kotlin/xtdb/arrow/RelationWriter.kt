package xtdb.arrow

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.util.closeAll
import xtdb.util.normalForm

interface RelationWriter : RelationReader {

    override var rowCount: Int

    override val vectors: Collection<VectorWriter>
    override fun vectorForOrNull(name: String): VectorWriter?
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    fun vectorFor(name: String, arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("vectorFor/2")
    override fun get(name: String) = vectorFor(name)

    fun endRow()

    fun append(rel: RelationReader) {
        rel.vectors.forEach { vectorFor(it.name, it.arrowType, it.nullable).append(it) }
        rowCount += rel.rowCount
    }

    fun writeRow(row: Map<*, *>) {
        row.forEach { (k, v) ->
            val normalKey = when (k) {
                is String -> k
                is Symbol -> normalForm(k).toString()
                is Keyword -> normalForm(k.sym).toString()
                else -> throw IllegalArgumentException("Column name must be a string, keyword or symbol")
            }

            val vType = v.toFieldType()
            val vector = vectorForOrNull(normalKey) ?: vectorFor(normalKey, vType.type, vType.isNullable)

            try {
                vector.writeObject(v)
            } catch (_: InvalidWriteObjectException) {
                val errType = v.toFieldType()
                vectorFor(normalKey, errType.type, errType.isNullable).writeObject(v)
            }
        }

        endRow()
    }

    fun writeRows(vararg rows: Map<*, *>) {
        rows.forEach(::writeRow)
    }

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    fun clear() {
        vectors.forEach { it.clear() }
        rowCount = 0
    }

    override fun close() {
        vectors.closeAll()
        rowCount = 0
    }
}