package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.RowCopier
import xtdb.arrow.Vector
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Companion.slice
import xtdb.arrow.VectorReader
import xtdb.arrow.unsupported
import xtdb.util.closeOnCatch

interface IVectorReader : VectorReader, AutoCloseable {
    override val nullable get() = this.field.isNullable
    override val fieldType: FieldType get() = this.field.fieldType

    override fun withName(newName: String): IVectorReader = RenamedVectorReader(this, newName)

    override fun vectorForOrNull(name: String): IVectorReader? = unsupported("vectorForOrNull")
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun get(name: String) = vectorFor(name)
    override fun get(idx: Int) = getObject(idx)

    override val keyNames: Set<String>? get() = unsupported("keyNames")

    override val listElements: IVectorReader get() = unsupported("listElements")

    override val mapKeys: IVectorReader get() = unsupported("mapKeys")
    override val mapValues: IVectorReader get() = unsupported("mapValues")

    override val legNames: Set<String>? get() = unsupported("legs")

    override fun openSlice(al: BufferAllocator): IVectorReader =
        field.createVector(al).closeOnCatch { v -> copyTo(v).withName(name) }

    override fun openMaterialisedSlice(al: BufferAllocator) =
        field.createVector(al).use { vec ->
            copyTo(vec)
            Vector.fromArrow(vec)
        }

    fun copyTo(vector: ValueVector): IVectorReader

    override fun select(idxs: IntArray): IVectorReader = IndirectVectorReader(this, selection(idxs))

    override fun select(startIdx: Int, len: Int): IVectorReader = IndirectVectorReader(this, slice(startIdx, len))

    fun rowCopier(writer: IVectorWriter): RowCopier
}