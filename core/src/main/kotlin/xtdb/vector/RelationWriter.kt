package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.Relation
import xtdb.arrow.Vector
import xtdb.arrow.VectorPosition

@Suppress("unused")
class RelationWriter(private val allocator: BufferAllocator) : IRelationWriter {
    private val wp = VectorPosition.build()
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

    override fun openAsRelation() = Relation(writers.values.map { Vector.fromArrow(it.vector) }, wp.position)

    override fun close() {
        writers.values.forEach(IVectorWriter::close)
    }
}
