package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType

class Utf8Vector(al: BufferAllocator, override val name: String, override var nullable: Boolean) : VariableWidthVector(al) {

    override val arrowType: ArrowType = Types.MinorType.VARCHAR.type

    override fun getObject0(idx: Int): String = getBytes(idx).toString(Charsets.UTF_8)

    override fun writeObject0(value: Any) = when {
        value is String -> writeBytes(value.toByteArray())
        else -> throw IllegalArgumentException("expecting string, got ${value::class.simpleName}")
    }
}
