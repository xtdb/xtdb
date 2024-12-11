package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn

class ByteVector(allocator: BufferAllocator, override var name: String, nullable: Boolean) :
    FixedWidthVector(allocator, nullable, MinorType.TINYINT.type, Byte.SIZE_BYTES) {

    override fun getByte(idx: Int) = getByte0(idx)
    override fun writeByte(value: Byte) = writeByte0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByte(idx)

    override fun writeObject0(value: Any) {
        if (value is Byte) writeByte(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun rowCopier0(src: VectorReader) =
        if (src !is ByteVector) throw InvalidCopySourceException(src.fieldType, fieldType)
        else {
            if (src.nullable && !nullable) nullable = true
            RowCopier { srcIdx ->
                valueCount.also { writeByte(src.getByte(srcIdx)) }
            }
        }
}