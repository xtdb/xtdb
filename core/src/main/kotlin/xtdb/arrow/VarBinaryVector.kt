package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Companion.VAR_BINARY
import xtdb.arrow.metadata.MetadataFlavour
import java.nio.ByteBuffer

class VarBinaryVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?,
    override val offsetBuffer: ExtensibleBuffer,
    override val dataBuffer: ExtensibleBuffer
) : VariableWidthVector(), MetadataFlavour.Bytes {

    constructor(al: BufferAllocator, name: String, nullable: Boolean) :
            this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val arrowType = VAR_BINARY_TYPE

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByteArray(idx)

    override fun writeObject0(value: Any) = when (value) {
        is ByteArray -> writeBytes(ByteBuffer.wrap(value))
        is ByteBuffer -> writeBytes(value)
        else -> throw InvalidWriteObjectException(this, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        VarBinaryVector(
            al, name, valueCount,
            validityBuffer?.openSlice(al), offsetBuffer.openSlice(al), dataBuffer.openSlice(al)
        )
}
