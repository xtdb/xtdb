package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

@Suppress("unused")
class RelationWriter(private val allocator: BufferAllocator) : IRelationWriter {
    private val wp = IVectorPosition.build()
    private val writers = mutableMapOf<String, IVectorWriter>()

    constructor(allocator: BufferAllocator, writers: List<IVectorWriter>) : this(allocator) {
        this.writers.putAll(writers.associateBy { it.vector.name })
    }

    override fun iterator() = writers.iterator()

    override fun writerPosition() = wp

    override fun startRow() = Unit

    override fun endRow() {
        val pos = ++wp.position
        writers.values.forEach { it.populateWithAbsents(pos) }
    }

    override fun colWriter(colName: String) = writers[colName] ?: colWriter(colName, UNION_FIELD_TYPE)

    override fun colWriter(colName: String, fieldType: FieldType) =
        writers[colName]
            ?.also { it.checkFieldType(fieldType) }

            ?: writerFor(fieldType.createNewSingleVector(colName, allocator, null))
                .also {
                    it.populateWithAbsents(wp.position)
                    writers[colName] = it
                }

    override fun maybePromote(colName: String, field: Field) : IVectorWriter {
        val writer = when (val existing = writers[colName]) {
            null -> colWriter(colName, field.fieldType).maybePromote(field)
            else -> existing.maybePromote(field)
        }
        writers[colName] = writer
        return  writer
    }

    override fun maybePromote(inRel: RelationReader) : IRelationWriter {
        inRel.forEach { vec -> maybePromote(vec.name, vec.field) }
        return this
    }

    override fun close() {
        writers.values.forEach(IVectorWriter::close)
    }
}
