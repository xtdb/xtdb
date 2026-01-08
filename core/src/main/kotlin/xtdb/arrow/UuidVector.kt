package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.vector.extensions.UuidType
import java.nio.ByteBuffer
import java.util.*

class UuidVector(override val inner: FixedSizeBinaryVector) : ExtensionVector(), MetadataFlavour.Bytes {

    override val arrowType = UuidType

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

        else -> throw InvalidWriteObjectException(arrowType, nullable, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) = UuidVector(inner.openSlice(al))
}