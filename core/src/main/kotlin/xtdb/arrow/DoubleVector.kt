package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

val F64: ArrowType = MinorType.FLOAT8.type

class DoubleVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val byteWidth = Double.SIZE_BYTES
    override val type: ArrowType = F64

    override fun getDouble(idx: Int) = getDouble0(idx)
    override fun writeDouble(v: Double) = writeDouble0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getDouble(idx)

    override fun writeObject0(value: Any) {
        if (value is Double) writeDouble(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeDouble(v.readDouble())

    override fun getMetaDouble(idx: Int) = getDouble(idx)

    override fun hashCode0(idx: Int, hasher: Hasher): Int = hasher.hash(getDouble(idx))

    override fun openSlice(al: BufferAllocator) =
        DoubleVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}