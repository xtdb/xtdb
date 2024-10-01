package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn

class ByteVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(allocator, Byte.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.TINYINT.type

    override fun getByte(idx: Int) = getByte0(idx)
    override fun writeByte(value: Byte) = writeByte0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByte(idx)

    override fun writeObject0(value: Any) {
        if (value is Byte) writeByte(value) else TODO("not a Byte")
    }

    override fun rowCopier0(src: VectorReader) =
        if (src !is ByteVector) TODO("promote ${src::class.simpleName}")
        else {
            if (src.nullable) nullable = true
            RowCopier { srcIdx ->
                valueCount.also { writeByte(src.getByte(srcIdx)) }
            }
        }
}