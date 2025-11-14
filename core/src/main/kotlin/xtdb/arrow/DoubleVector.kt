package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class DoubleVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val byteWidth = Double.SIZE_BYTES
    override val arrowType: ArrowType = F64.arrowType

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
        DoubleVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}