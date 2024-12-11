package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn

class FloatVector(
    allocator: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : FixedWidthVector(allocator, nullable, MinorType.FLOAT4.type, Float.SIZE_BYTES) {

    override fun getFloat(idx: Int) = getFloat0(idx)
    override fun writeFloat(value: Float) = writeFloat0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getFloat(idx)

    override fun writeObject0(value: Any) {
        if (value is Float) writeFloat(value) else throw InvalidWriteObjectException(fieldType, value)
    }
}