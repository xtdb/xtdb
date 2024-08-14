package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn

class FixedSizeBinaryVector(
    al: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
    byteWidth: Int
) : FixedWidthVector(al, byteWidth) {

    override val arrowType = ArrowType.FixedSizeBinary(byteWidth)

    override fun getBytes(idx: Int) = getBytes0(idx)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getBytes(idx)

    override fun writeObject0(value: Any) = when (value) {
        is ByteArray -> writeBytes(value)
        else -> TODO("unknown type: ${value::class.simpleName}")
    }
}
