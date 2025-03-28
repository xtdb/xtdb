package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class LongVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val type: ArrowType = MinorType.BIGINT.type
    override val byteWidth = Long.SIZE_BYTES

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getLong(idx)

    override fun writeObject0(value: Any) {
        if (value is Long) writeLong(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun getMetaDouble(idx: Int) = getLong(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getLong(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        LongVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}