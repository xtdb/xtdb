package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import java.nio.ByteBuffer

class VarBinaryVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer,
    override val offsetBuffer: ExtensibleBuffer,
    override val dataBuffer: ExtensibleBuffer
) : VariableWidthVector() {

    constructor(al: BufferAllocator, name: String, nullable: Boolean) :
            this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val type: ArrowType = Types.MinorType.VARBINARY.type

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByteArray(idx)

    override fun writeObject0(value: Any) = when (value) {
        is ByteArray -> writeBytes(ByteBuffer.wrap(value))
        is ByteBuffer -> writeBytes(value)
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun openSlice(al: BufferAllocator) =
        VarBinaryVector(
            name, nullable, valueCount,
            validityBuffer.openSlice(al), offsetBuffer.openSlice(al), dataBuffer.openSlice(al)
        )
}
