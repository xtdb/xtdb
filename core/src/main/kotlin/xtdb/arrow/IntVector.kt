package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

internal val I32 = MinorType.INT.type

class IntVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    override val type: ArrowType = I32
    override val byteWidth = Int.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getInt(idx)

    override fun writeObject0(value: Any) {
        if (value is Int) writeInt(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun getMetaDouble(idx: Int) = getInt(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getInt(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        IntVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}