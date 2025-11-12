package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import java.nio.ByteBuffer

internal val UTF8_TYPE = MinorType.VARCHAR.type

class Utf8Vector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?,
    override val offsetBuffer: ExtensibleBuffer,
    override val dataBuffer: ExtensibleBuffer
) : VariableWidthVector(), MetadataFlavour.Bytes {

    constructor(al: BufferAllocator, name: String, nullable: Boolean) :
            this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val arrowType: ArrowType = UTF8_TYPE

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): String = getByteArray(idx).toString(Charsets.UTF_8)

    override fun writeObject0(value: Any) = when {
        value is String -> writeBytes(ByteBuffer.wrap(value.toByteArray()))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        Utf8Vector(
            al, name, valueCount,
            validityBuffer?.openSlice(al), offsetBuffer.openSlice(al), dataBuffer.openSlice(al)
        )
}
