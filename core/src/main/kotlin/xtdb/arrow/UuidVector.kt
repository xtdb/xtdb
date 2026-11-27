package xtdb.arrow

import java.time.ZoneId
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.arrow.extensions.UuidType
import xtdb.hash.perRowHashes
import xtdb.util.Hasher
import java.nio.ByteBuffer
import java.util.*

class UuidVector(override val inner: FixedSizeBinaryVector) : ExtensionVector(), MetadataFlavour.Bytes {

    override val arrowType = UuidType
    override val monoType = Scalar(arrowType)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) =
        ByteBuffer.wrap(inner.getObject0(idx, keyFn)).let { buf ->
            UUID(buf.getLong(), buf.getLong())
        }

    override fun writeObject0(value: Any) = when (value) {
        is UUID ->
            writeBytes(ByteBuffer.allocate(16).run {
                putLong(value.mostSignificantBits)
                putLong(value.leastSignificantBits)
                flip()
            })

        else -> throw InvalidWriteObjectException(this, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = UuidVector(inner.openSlice(al))

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) = perRowHashes(this, out, sel, hasher)

    override fun equiRows(other: Vector, zone: ZoneId) = if (other.isBytes) equiBytes(this, other) else null
    override fun compareRows(other: Vector, zone: ZoneId) = if (other is UuidVector) compareBytes(this, other) else null
}