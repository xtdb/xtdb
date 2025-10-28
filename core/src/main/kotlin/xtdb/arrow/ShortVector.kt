package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class ShortVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: BitBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    override val type: ArrowType = MinorType.SMALLINT.type
    override val byteWidth = Short.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, BitBuffer(al), ExtensibleBuffer(al))

    override fun getShort(idx: Int) = getShort0(idx)
    override fun writeShort(v: Short) = writeShort0(v)
    override fun getInt(idx: Int) = getShort(idx).toInt()
    override fun getLong(idx: Int) = getShort(idx).toLong()

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getShort(idx)

    override fun writeObject0(value: Any) {
        if (value is Short) writeShort(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeShort(v.readShort())

    override fun getMetaDouble(idx: Int) = getShort(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getShort(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        ShortVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}