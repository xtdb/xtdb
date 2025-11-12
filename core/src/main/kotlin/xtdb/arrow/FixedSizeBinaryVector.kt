package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import java.lang.Long.reverseBytes
import java.nio.ByteBuffer

class FixedSizeBinaryVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override val byteWidth: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), LongLongVectorReader, MetadataFlavour.Bytes {

    override val arrowType = ArrowType.FixedSizeBinary(byteWidth)

    constructor(al: BufferAllocator, name: String, nullable: Boolean, byteWidth: Int)
            : this(al, name, 0, byteWidth, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getBytes(idx: Int): ByteBuffer = getBytes0(idx)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByteArray(idx)

    override fun getLongLongHigh(idx: Int) = reverseBytes(dataBuffer.getLong(idx * 2))
    override fun getLongLongLow(idx: Int) = reverseBytes(dataBuffer.getLong(idx * 2 + 1))

    override fun writeObject0(value: Any) = when (value) {
        is ByteBuffer -> writeBytes(value)
        is ByteArray -> writeBytes(ByteBuffer.wrap(value))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        FixedSizeBinaryVector(al, name, valueCount, byteWidth, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}
