package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types
import xtdb.api.query.IKeyFn
import java.nio.ByteBuffer

class VarBinaryVector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : VariableWidthVector(al, nullable, Types.MinorType.VARBINARY.type) {

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByteArray(idx)

    override fun writeObject0(value: Any) = when (value) {
        is ByteArray -> writeBytes(ByteBuffer.wrap(value))
        is ByteBuffer -> writeBytes(value)
        else -> throw InvalidWriteObjectException(fieldType, value)
    }
}
