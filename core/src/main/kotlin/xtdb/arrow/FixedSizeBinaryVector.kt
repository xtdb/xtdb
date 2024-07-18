package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType

class FixedSizeBinaryVector(
    al: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
    val byteWidth: Int
) : FixedWidthVector(al) {

    override val arrowType = ArrowType.FixedSizeBinary(byteWidth)

    override fun getObject0(idx: Int) = getBytes0(idx, byteWidth)

    override fun writeObject0(value: Any) = when (value) {
        is ByteArray -> writeBytes(value)
        else -> TODO("unknown type: ${value::class.simpleName}")
    }
}
