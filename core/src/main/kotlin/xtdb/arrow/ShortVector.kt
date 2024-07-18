package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType

class ShortVector(allocator: BufferAllocator, override val name: String, override var nullable: Boolean) : FixedWidthVector(allocator) {

    override val arrowType: ArrowType = MinorType.SMALLINT.type

    override fun writeNull() {
        super.writeNull()
        writeShort0(0)
    }

    override fun getShort(idx: Int) = getShort0(idx)
    override fun writeShort(value: Short) = writeShort0(value)

    override fun getObject0(idx: Int) = getShort(idx)

    override fun writeObject0(value: Any) {
        if (value is Short) writeShort(value) else TODO("not a Short")
    }
}