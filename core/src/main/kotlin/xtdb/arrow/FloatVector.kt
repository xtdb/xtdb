package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn

class FloatVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(allocator, Float.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.FLOAT4.type

    override fun getFloat(idx: Int) = getFloat0(idx)
    override fun writeFloat(value: Float) = writeFloat0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getFloat(idx)

    override fun writeObject0(value: Any) {
        if (value is Float) writeFloat(value) else TODO("not a Float")
    }
}