package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.RowCopier
import xtdb.arrow.Vector
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Companion.slice
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.arrow.unsupported
import xtdb.util.closeOnCatch

interface IVectorReader : VectorReader, AutoCloseable {
    override val nullable get() = this.field.isNullable
    override val fieldType: FieldType get() = this.field.fieldType

    override fun withName(newName: String): VectorReader = RenamedVectorReader(this, newName)

    override val keyNames: Set<String>? get() = unsupported("keyNames")
    override val legNames: Set<String>? get() = unsupported("legs")

    override fun openSlice(al: BufferAllocator): VectorReader =
        field.createVector(al).closeOnCatch { v -> copyTo(v).withName(name) }

    override fun openDirectSlice(al: BufferAllocator) =
        field.createVector(al).use { vec ->
            copyTo(vec)
            Vector.fromArrow(vec)
        }

    fun copyTo(vector: ValueVector): VectorReader

    override fun select(idxs: IntArray): VectorReader = IndirectVectorReader(this, selection(idxs))

    override fun select(startIdx: Int, len: Int): VectorReader = IndirectVectorReader(this, slice(startIdx, len))
}