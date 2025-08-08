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

    override fun vectorForOrNull(name: String): IVectorWriter?
    override fun vectorFor(name: String): IVectorWriter = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun vectorFor(name: String, fieldType: FieldType): IVectorWriter

    override fun rowCopier(rel: RelationReader): RowCopier {
        val copiers = rel.vectors.map {
            it.rowCopier(vectorFor(it.name, UNION_FIELD_TYPE))
        }

        return RowCopier { srcIdx ->
            copiers.forEach { it.copyRow(srcIdx) }
            endRow()
        }
    }

    override fun openSlice(al: BufferAllocator) = asReader.openSlice(al)
    override fun openDirectSlice(al: BufferAllocator) = asReader.openDirectSlice(al)

    override val asReader get() = RelationReader.from(vectors.map { it.asReader }, rowCount)
}
