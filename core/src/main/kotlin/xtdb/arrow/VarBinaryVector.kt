package xtdb.arrow

import java.time.ZoneId
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Companion.VAR_BINARY
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.hash.variableWidthHashes
import xtdb.util.Hasher
import java.nio.ByteBuffer

class VarBinaryVector private constructor(
    override val al: BufferAllocator,
    override var valueCount: Int,
    override var validityBuffer: BitBuffer?,
    override val offsetBuffer: ExtensibleBuffer,
    override val dataBuffer: ExtensibleBuffer
) : VariableWidthVector(), MetadataFlavour.Bytes {

    constructor(al: BufferAllocator, nullable: Boolean) :
            this(al, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), ExtensibleBuffer.withMaxCapacity(al, MAX_DATA_BYTES))

    override var lastOffset = if (valueCount == 0) 0 else offsetBuffer.getInt(valueCount)

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
            al, valueCount,
            validityBuffer?.openSlice(al), offsetBuffer.openSlice(al), dataBuffer.openSlice(al)
        )

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) = variableWidthHashes(this, out, sel, hasher)

    override fun equiRows(other: Vector, zone: ZoneId) = if (other.isBytes) equiBytes(this, other) else null
    override fun compareRows(other: Vector, zone: ZoneId) = if (other is VarBinaryVector || other is FixedSizeBinaryVector) compareBytes(this, other) else null
}
