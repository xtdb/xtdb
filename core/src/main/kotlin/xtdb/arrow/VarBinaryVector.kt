package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType

class VarBinaryVector(al: BufferAllocator, override val name: String, override var nullable: Boolean) : VariableWidthVector(al) {

    override val arrowType: ArrowType = Types.MinorType.VARBINARY.type

    override fun getObject0(idx: Int) = getBytes(idx)

    override fun writeObject0(value: Any) = when {
        value is ByteArray -> writeBytes(value)
        else -> TODO("unknown type: ${value::class.simpleName}")
    }
}
