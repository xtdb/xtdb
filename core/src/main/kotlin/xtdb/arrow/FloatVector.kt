package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher

class FloatVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector() {

    override val byteWidth = Float.SIZE_BYTES
    override val type: ArrowType = MinorType.FLOAT4.type

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getFloat(idx: Int) = getFloat0(idx)
    override fun writeFloat(value: Float) = writeFloat0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getFloat(idx)

    override fun writeObject0(value: Any) {
        if (value is Float) writeFloat(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher): Int = hasher.hash(getFloat(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        FloatVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}