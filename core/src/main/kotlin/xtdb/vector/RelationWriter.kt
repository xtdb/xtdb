package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.RelationReader

@Suppress("unused")
class RelationWriter(private val allocator: BufferAllocator) : IRelationWriter {
    private val writers = mutableMapOf<String, IVectorWriter>()

    constructor(allocator: BufferAllocator, writers: List<IVectorWriter>) : this(allocator) {
        this.writers.putAll(writers.associateBy { it.vector.name })
    }

    override fun iterator() = writers.iterator()

    override var rowCount = 0
    override val vectors: Collection<IVectorWriter> get() = writers.values

    override fun endRow(): Int {
        val pos = rowCount++
        writers.values.forEach { it.populateWithAbsents(rowCount) }
        return pos
    }

    override fun vectorForOrNull(name: String) = writers[name]

    override fun vectorFor(name: String, fieldType: FieldType) =
        writers[name]
            ?.also { it.checkFieldType(fieldType) }

            ?: writerFor(fieldType.createNewSingleVector(name, allocator, null))
                .also {
                    it.populateWithAbsents(rowCount)
                    writers[name] = it
                }

    override val asReader get() = RelationReader.from(vectors.map { it.asReader }, rowCount)

    override fun close() {
        writers.values.forEach(IVectorWriter::close)
    }
}
