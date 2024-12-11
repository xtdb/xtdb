package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn
import java.nio.ByteBuffer

internal val UTF8_TYPE = MinorType.VARCHAR.type

class Utf8Vector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : VariableWidthVector(al, nullable, UTF8_TYPE) {

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): String = getByteArray(idx).toString(Charsets.UTF_8)

    override fun writeObject0(value: Any) = when {
        value is String -> writeBytes(ByteBuffer.wrap(value.toByteArray()))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }
}
