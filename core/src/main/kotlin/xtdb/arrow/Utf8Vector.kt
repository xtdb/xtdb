package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import java.nio.ByteBuffer

class Utf8Vector(al: BufferAllocator, override val name: String, override var nullable: Boolean) : VariableWidthVector(al) {

    override val arrowType: ArrowType = Types.MinorType.VARCHAR.type

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): String = getByteArray(idx).toString(Charsets.UTF_8)

    override fun writeObject0(value: Any) = when {
        value is String -> writeBytes(ByteBuffer.wrap(value.toByteArray()))
        else -> throw IllegalArgumentException("expecting string, got ${value::class.simpleName}")
    }
}
