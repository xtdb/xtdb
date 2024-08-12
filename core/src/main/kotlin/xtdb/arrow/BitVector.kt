package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType

class BitVector(al: BufferAllocator, override val name: String, override var nullable: Boolean ) : FixedWidthVector(al,0) {

    override val arrowType: ArrowType = MinorType.BIT.type

    override fun getBoolean(idx: Int) = getBoolean0(idx)
    override fun writeBoolean(value: Boolean) = writeBoolean0(value)

    override fun getObject0(idx: Int) = getBoolean(idx)

    override fun writeObject0(value: Any) {
        if (value is Boolean) writeBoolean(value) else TODO("not a Boolean")
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = if (getBoolean(idx)) 17 else 19
}