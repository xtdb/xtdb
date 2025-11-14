package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class ShortVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : IntegerVector(), MetadataFlavour.Number {

    override val arrowType: ArrowType = MinorType.SMALLINT.type
    override val byteWidth = Short.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getShort(idx: Int) = getShort0(idx)
    override fun writeShort(v: Short) = writeShort0(v)
    override fun getInt(idx: Int) = getShort(idx).toInt()
    override fun getLong(idx: Int) = getShort(idx).toLong()

    override fun getAsInt(idx: Int) = getShort(idx).toInt()
    override fun getAsLong(idx: Int) = getShort(idx).toLong()
    override fun getAsFloat(idx: Int) = getShort(idx).toFloat()
    override fun getAsDouble(idx: Int) = getShort(idx).toDouble()

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getShort(idx)

    override fun writeObject0(value: Any) {
        if (value is Short) writeShort(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeShort(v.readShort())

    override fun getMetaDouble(idx: Int) = getShort(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getShort(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        ShortVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}