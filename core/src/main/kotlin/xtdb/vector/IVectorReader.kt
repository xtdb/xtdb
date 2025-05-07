package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.*
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Companion.slice
import xtdb.util.closeOnCatch

interface IVectorReader : VectorReader, AutoCloseable {
    override val nullable get() = this.field.isNullable
    override val fieldType: FieldType get() = this.field.fieldType

    override fun withName(newName: String): IVectorReader = RenamedVectorReader(this, newName)

    fun structKeyReader(colName: String): IVectorReader? = unsupported("structKeyReader")
    override val keyNames: Set<String>? get() = unsupported("keyNames")

    override val listElements: IVectorReader get() = unsupported("listElements")

    override val mapKeys: IVectorReader get() = unsupported("mapKeys")
    override val mapValues: IVectorReader get() = unsupported("mapValues")

    fun legReader(legKey: String): IVectorReader? = unsupported("legReader")

    override val legNames: Set<String>? get() = unsupported("legs")


    fun copy(allocator: BufferAllocator): IVectorReader = field.createVector(allocator).closeOnCatch { v -> copyTo(v).withName(name) }
    fun copyTo(vector: ValueVector): IVectorReader

    override fun select(idxs: IntArray): IVectorReader = IndirectVectorReader(this, selection(idxs))

    override fun select(startIdx: Int, len: Int): IVectorReader = IndirectVectorReader(this, slice(startIdx, len))

    fun rowCopier(writer: IVectorWriter): RowCopier
}