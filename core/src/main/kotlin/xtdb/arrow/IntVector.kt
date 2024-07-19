package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType

class IntVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(allocator, Int.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.INT.type

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int) = getInt(idx)

    override fun writeObject0(value: Any) {
        if (value is Int) writeInt(value) else TODO("not an Int")
    }
}