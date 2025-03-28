package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import java.nio.ByteBuffer

internal val UTF8_TYPE = MinorType.VARCHAR.type

class Utf8Vector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer,
    override val offsetBuffer: ExtensibleBuffer,
    override val dataBuffer: ExtensibleBuffer
) : VariableWidthVector(), MetadataFlavour.Bytes {

    constructor(al: BufferAllocator, name: String, nullable: Boolean) :
            this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val type: ArrowType = UTF8_TYPE

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): String = getByteArray(idx).toString(Charsets.UTF_8)

    override fun writeObject0(value: Any) = when {
        value is String -> writeBytes(ByteBuffer.wrap(value.toByteArray()))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        Utf8Vector(
            name, nullable, valueCount,
            validityBuffer.openSlice(al), offsetBuffer.openSlice(al), dataBuffer.openSlice(al)
        )
}
