package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import org.apache.arrow.vector.types.pojo.ArrowType.Bool.INSTANCE as BIT_TYPE

class BitVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector() {

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean
    ) : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val byteWidth = 0
    override val type: ArrowType = BIT_TYPE

    override fun getBoolean(idx: Int) = getBoolean0(idx)
    override fun writeBoolean(value: Boolean) = writeBoolean0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getBoolean(idx)

    override fun writeObject0(value: Any) {
        if (value is Boolean) writeBoolean(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = if (getBoolean(idx)) 17 else 19

    override fun rowCopier0(src: VectorReader) =
        if (src !is BitVector) throw InvalidCopySourceException(src.fieldType, fieldType)
        else RowCopier { srcIdx ->
            if (src.nullable && !nullable) nullable = true
            valueCount.apply { if (src.isNull(srcIdx)) writeNull() else writeBoolean(src.getBoolean(srcIdx)) }
        }

    override fun openSlice(al: BufferAllocator) =
        BitVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}