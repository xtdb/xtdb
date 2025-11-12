package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class FloatVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    override val byteWidth = Float.SIZE_BYTES
    override val arrowType: ArrowType = MinorType.FLOAT4.type

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getFloat(idx: Int) = getFloat0(idx)
    override fun writeFloat(v: Float) = writeFloat0(v)
    override fun getDouble(idx: Int) = getFloat(idx).toDouble()

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getFloat(idx)

    override fun writeObject0(value: Any) {
        if (value is Float) writeFloat(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeFloat(v.readFloat())

    override fun getMetaDouble(idx: Int) = getFloat(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher): Int = hasher.hash(getFloat(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        FloatVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}