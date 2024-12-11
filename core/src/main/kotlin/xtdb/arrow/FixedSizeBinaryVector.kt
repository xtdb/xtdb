package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import java.nio.ByteBuffer

class FixedSizeBinaryVector(
    al: BufferAllocator,
    override val name: String,
    nullable: Boolean,
    byteWidth: Int
) : FixedWidthVector(al, nullable, ArrowType.FixedSizeBinary(byteWidth), byteWidth) {

    override fun getBytes(idx: Int): ByteBuffer = getBytes0(idx)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByteArray(idx)

    override fun writeObject0(value: Any) = when (value) {
        is ByteBuffer -> writeBytes(value)
        is ByteArray -> writeBytes(ByteBuffer.wrap(value))
        else -> TODO("unknown type: ${value::class.simpleName}")
    }
}
