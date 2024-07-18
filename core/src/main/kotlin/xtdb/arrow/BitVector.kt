package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType

class BitVector(al: BufferAllocator, override val name: String, override var nullable: Boolean ) : FixedWidthVector(al) {

    override val arrowType = MinorType.BIT.type

    override fun getBoolean(idx: Int) = getBoolean0(idx)
    override fun writeBoolean(value: Boolean) = writeBoolean0(value)

    override fun getObject0(idx: Int) = getBoolean(idx)

    override fun writeObject0(value: Any) {
        if (value is Boolean) writeBoolean(value) else TODO("not a Boolean")
    }
}