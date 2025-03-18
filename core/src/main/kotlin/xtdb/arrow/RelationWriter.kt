package xtdb.arrow

import clojure.lang.Keyword
import xtdb.util.normalForm

interface RelationWriter<V : VectorWriter> : Iterable<V>, RelationReader<V> {
    override operator fun get(colName: String): V

    fun endRow(): Int

    fun rowCopier(rel: RelationReader<*>): RowCopier

    fun append(rel: RelationReader<*>) {
        val copier = rowCopier(rel)
        repeat(rel.rowCount) { copier.copyRow(it) }
    }

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    fun clear()

    fun writeRows(vararg rows: Map<*, *>) {
        rows.forEach { row ->
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
    }
}