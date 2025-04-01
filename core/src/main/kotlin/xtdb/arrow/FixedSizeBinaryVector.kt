package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import java.nio.ByteBuffer

class FixedSizeBinaryVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val byteWidth: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Bytes {

    override val type = ArrowType.FixedSizeBinary(byteWidth)

    constructor(al: BufferAllocator, name: String, nullable: Boolean, byteWidth: Int)
            : this(name, nullable, 0, byteWidth, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getBytes(idx: Int): ByteBuffer = getBytes0(idx)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByteArray(idx)

    override fun writeObject0(value: Any) = when (value) {
        is ByteBuffer -> writeBytes(value)
        is ByteArray -> writeBytes(ByteBuffer.wrap(value))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        FixedSizeBinaryVector(name, nullable, valueCount, byteWidth, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}
