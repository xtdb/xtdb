package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.RowCopier
import xtdb.arrow.VectorWriter

@Suppress("unused")
class OldRelationWriter(private val allocator: BufferAllocator) : RelationWriter, Iterable<Map.Entry<String, VectorWriter>> {
    private val writers = mutableMapOf<String, VectorWriter>()

    constructor(allocator: BufferAllocator, writers: List<VectorWriter>) : this(allocator) {
        this.writers.putAll(writers.associateBy { it.name })
    }

    constructor(allocator: BufferAllocator, schema: Schema) : this(allocator) {
        writers.putAll(schema.fields.associate { it.name to writerFor(it.createVector(allocator)) })
    }

    override fun iterator() = writers.iterator()

    /**
     * Maintains the next position to be written to.
     *
     * This is incremented either by using the [RelationWriter.rowCopier], or by explicitly calling [RelationWriter.endRow]
     */
    override var rowCount = 0
    override val vectors: Collection<VectorWriter> get() = writers.values

    override fun endRow(): Int {
        val pos = rowCount++
        vectors.forEach { it.populateWithAbsents(rowCount) }
        return pos
    }

    override fun vectorForOrNull(name: String) = writers[name]

    override fun vectorFor(name: String, fieldType: FieldType) =
        // HACK we don't check nor promote here, because RootWriter didn't
        writers[name]
            ?: writerFor(fieldType.createNewSingleVector(name, allocator, null))
                .also {
                    it.populateWithAbsents(rowCount)
                    writers[name] = it
                }

    override fun openSlice(al: BufferAllocator) = asReader.openSlice(al)
    override fun openDirectSlice(al: BufferAllocator) = asReader.openDirectSlice(al)

    override val asReader get() = RelationReader.from(vectors.map { it.asReader }, rowCount)
}
