package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType

class DoubleVector(allocator: BufferAllocator, override val name: String, override var nullable: Boolean) : FixedWidthVector(allocator) {

    override val arrowType: ArrowType = MinorType.FLOAT4.type

    override fun writeNull() {
        super.writeNull()
        writeDouble0(0.0)
    }

    override fun getDouble(idx: Int) = getDouble0(idx)
    override fun writeDouble(value: Double) = writeDouble0(value)

    override fun getObject0(idx: Int) = getDouble(idx)

    override fun writeObject0(value: Any) {
        if (value is Double) writeDouble(value) else TODO("not a Double")
    }
}