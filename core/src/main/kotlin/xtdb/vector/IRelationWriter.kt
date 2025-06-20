package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.RowCopier

interface IRelationWriter : RelationWriter, AutoCloseable, Iterable<Map.Entry<String, IVectorWriter>> {
    /**
     * Maintains the next position to be written to.
     *
     * This is incremented either by using the [IRelationWriter.rowCopier], or by explicitly calling [IRelationWriter.endRow]
     */
    override var rowCount: Int
    override val schema get() = Schema(vectors.map { it.field })

    override fun endRow(): Int

    override fun vectorForOrNull(name: String): IVectorWriter?
    override fun vectorFor(name: String): IVectorWriter = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun vectorFor(name: String, fieldType: FieldType): IVectorWriter

    /**
     * This method syncs the value counts on the underlying writers/root (e.g. [org.apache.arrow.vector.VectorSchemaRoot.setRowCount])
     * so that all the values written become visible through the Arrow Java API.
     * We don't call this after every write because (for composite vectors, and especially unions) it's not the cheapest call.
     */
    fun syncRowCount() = this.forEach { it.value.syncValueCount() }

    override fun rowCopier(rel: RelationReader): RowCopier {
        val copiers = rel.vectors.map {
            it.rowCopier(vectorFor(it.name, UNION_FIELD_TYPE))
        }

        return RowCopier { srcIdx ->
            copiers.forEach { it.copyRow(srcIdx) }
            endRow()
        }
    }

    override fun append(rel: RelationReader) {
        rel.vectors.forEach { vectorFor(it.name, it.fieldType).append(it) }
        rowCount += rel.rowCount
    }

    override fun openSlice(al: BufferAllocator) = toReader().openSlice(al)
    override fun openDirectSlice(al: BufferAllocator) = toReader().openDirectSlice(al)

    fun toReader() =
        RelationReader.from(this.map { ValueVectorReader.from(it.value.apply { syncValueCount() }.vector) }, rowCount)

    override fun clear() {
        this.forEach { it.value.clear() }
        rowCount = 0
    }
}
