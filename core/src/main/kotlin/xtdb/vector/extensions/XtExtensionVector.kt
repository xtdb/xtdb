package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ExtensionTypeVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.util.TransferPair

abstract class XtExtensionVector<V : FieldVector>(private val field: Field, allocator: BufferAllocator, underlyingVector: V) :
    ExtensionTypeVector<V>(field, allocator, underlyingVector) {

    constructor(name: String, allocator: BufferAllocator, fieldType: FieldType, underlyingVector: V)
        : this(Field(name, fieldType, null), allocator, underlyingVector)

    override fun getField(): Field = field

    override fun hashCode(index: Int): Int = underlyingVector.hashCode(index)
    override fun hashCode(index: Int, hasher: ArrowBufHasher): Int = underlyingVector.hashCode(index, hasher)

    override fun copyFromSafe(fromIndex: Int, thisIndex: Int, from: ValueVector) {
        underlyingVector.copyFromSafe(fromIndex, thisIndex, (from as XtExtensionVector<*>).underlyingVector)
    }

    abstract fun getObject0(index: Int) : Any

    override fun getObject(index: Int): Any? = if (isNull(index)) null else getObject0(index)

    override fun getTransferPair(field: Field, allocator: BufferAllocator) =
        makeTransferPair(field.createVector(allocator))

    override fun makeTransferPair(target: ValueVector): TransferPair {
        val targetUnderlying: ValueVector = (target as XtExtensionVector<*>).underlyingVector
        val tp = underlyingVector.makeTransferPair(targetUnderlying)

        return object : TransferPair {
            override fun transfer() = tp.transfer()
            override fun splitAndTransfer(startIndex: Int, length: Int) = tp.splitAndTransfer(startIndex, length)
            override fun getTo(): ValueVector = target
            override fun copyValueSafe(from: Int, to: Int) = tp.copyValueSafe(from, to)
        }
    }
}
