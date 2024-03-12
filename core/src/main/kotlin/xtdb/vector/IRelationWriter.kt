package xtdb.vector

import org.apache.arrow.vector.types.pojo.FieldType

interface IRelationWriter : AutoCloseable, Iterable<Map.Entry<String, IVectorWriter>> {
    /**
     * Maintains the next position to be written to.
     *
     * This is incremented either by using the [IRelationWriter.rowCopier], or by explicitly calling [IRelationWriter.endRow]
     */
    fun writerPosition(): IVectorPosition

    fun startRow()
    fun endRow()

    /**
     * This method syncs the value counts on the underlying writers/root (e.g. [org.apache.arrow.vector.VectorSchemaRoot.setRowCount])
     * so that all of the values written become visible through the Arrow Java API.
     * We don't call this after every write because (for composite vectors, and especially unions) it's not the cheapest call.
     */
    fun syncRowCount() = this.forEach { it.value.syncValueCount() }

    fun colWriter(colName: String): IVectorWriter

    fun colWriter(colName: String, fieldType: FieldType): IVectorWriter

    fun rowCopier(inRel: RelationReader): IRowCopier {
        val copiers = inRel.map { inVec -> inVec.rowCopier(colWriter(inVec.name)) }

        return IRowCopier { srcIdx ->
            val pos = writerPosition().position

            startRow()
            copiers.forEach { it.copyRow(srcIdx) }
            endRow()

            pos
        }
    }

    fun clear() {
        this.forEach { it.value.clear() }
        writerPosition().position = 0
    }
}
