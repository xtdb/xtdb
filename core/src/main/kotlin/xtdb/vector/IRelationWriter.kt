package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.Relation
import xtdb.arrow.RowCopier
import xtdb.util.normalForm

interface IRelationWriter : AutoCloseable, Iterable<Map.Entry<String, IVectorWriter>> {
    /**
     * Maintains the next position to be written to.
     *
     * This is incremented either by using the [IRelationWriter.rowCopier], or by explicitly calling [IRelationWriter.endRow]
     */
    var rowCount: Int

    fun startRow()
    fun endRow()

    fun writeRow(row: Map<*, *>?) {
        if (row == null) return

        startRow()
        row.forEach { (colName, value) ->
            colWriter(
                when (colName) {
                    is String -> colName
                    is Keyword -> normalForm(colName.sym).toString()
                    else -> throw IllegalArgumentException("Column name must be a string or keyword")
                }
            ).writeObject(value)
        }
        endRow()
    }

    fun writeRows(rows: List<Map<*, *>?>?) = rows?.forEach { writeRow(it) }

    /**
     * This method syncs the value counts on the underlying writers/root (e.g. [org.apache.arrow.vector.VectorSchemaRoot.setRowCount])
     * so that all the values written become visible through the Arrow Java API.
     * We don't call this after every write because (for composite vectors, and especially unions) it's not the cheapest call.
     */
    fun syncRowCount() = this.forEach { it.value.syncValueCount() }

    fun colWriter(colName: String): IVectorWriter

    fun colWriter(colName: String, fieldType: FieldType): IVectorWriter

    fun rowCopier(inRel: RelationReader): RowCopier {
        val copiers = inRel.vectors.map { inVec -> inVec.rowCopier(colWriter(inVec.name)) }

        return RowCopier { srcIdx ->
            val pos = rowCount

            startRow()
            copiers.forEach { it.copyRow(srcIdx) }
            endRow()

            pos
        }
    }

    fun openAsRelation(): Relation

    fun toReader() =
        RelationReader.from(this.map { ValueVectorReader.from(it.value.apply { syncValueCount() }.vector) }, rowCount)

    fun clear() {
        this.forEach { it.value.clear() }
        rowCount = 0
    }
}
