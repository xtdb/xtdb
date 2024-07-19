package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType

class LongVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(allocator, Long.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.BIGINT.type

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int) = getLong(idx)

    override fun writeObject0(value: Any) {
        if (value is Long) writeLong(value) else TODO("not a Long")
    }
}