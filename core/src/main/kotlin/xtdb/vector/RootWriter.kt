package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader

class RootWriter(private val root: VectorSchemaRoot) : IRelationWriter {
    private val writers: MutableMap<String, IVectorWriter> =
        root.fieldVectors.associateTo(mutableMapOf()) { it.name to writerFor(it) }

    override var rowCount = 0
    override val vectors get() = writers.values

    override fun iterator() = writers.entries.iterator()

    override fun endRow(): Int {
        val pos = rowCount++
        writers.values.forEach { it.populateWithAbsents(rowCount) }
        return pos
    }

    override fun vectorForOrNull(name: String) = writers[name]

    // dynamic column creation unsupported in RootWriters
    override fun vectorFor(name: String, fieldType: FieldType) = vectorFor(name)

    override fun syncRowCount() {
        root.syncSchema()
        root.rowCount = rowCount

        writers.values.forEach { it.syncValueCount() }
    }

    override fun openDirectSlice(al: BufferAllocator): Relation {
        syncRowCount()
        return Relation.fromRoot(al, root)
    }

    override fun close() {
        writers.values.forEach { it.close() }
    }

    override val asReader get() = RelationReader.from(vectors.map { it.asReader }, rowCount)
}
