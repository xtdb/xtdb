package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class ByteVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean
    ) : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val byteWidth = Byte.SIZE_BYTES
    override val type: ArrowType = MinorType.TINYINT.type

    override fun getByte(idx: Int) = getByte0(idx)
    override fun writeByte(v: Byte) = writeByte0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByte(idx)

    override fun writeObject0(value: Any) {
        if (value is Byte) writeByte(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeByte(v.readByte())

    override fun getMetaDouble(idx: Int) = getByte(idx).toDouble()

    override fun rowCopier0(src: VectorReader): RowCopier {
        check(src is ByteVector)
        if (src.nullable && !nullable) nullable = true
        return RowCopier { srcIdx ->
            valueCount.also { writeByte(src.getByte(srcIdx)) }
        }
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getByte(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        ByteVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}