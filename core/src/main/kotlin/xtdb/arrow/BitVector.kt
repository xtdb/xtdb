package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import xtdb.api.query.IKeyFn
import org.apache.arrow.vector.types.pojo.ArrowType.Bool.INSTANCE as BIT_TYPE

class BitVector(al: BufferAllocator, override val name: String, nullable: Boolean) :
    FixedWidthVector(al, nullable, BIT_TYPE, 0) {

    override fun getBoolean(idx: Int) = getBoolean0(idx)
    override fun writeBoolean(value: Boolean) = writeBoolean0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getBoolean(idx)

    override fun writeObject0(value: Any) {
        if (value is Boolean) writeBoolean(value) else TODO("not a Boolean")
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = if (getBoolean(idx)) 17 else 19

    override fun rowCopier0(src: VectorReader) =
        if (src !is BitVector) TODO("promote ${src::class.simpleName}")
        else RowCopier { srcIdx ->
            if (src.nullable && !nullable) TODO("promote to nullable")
            valueCount.apply { if (src.isNull(srcIdx)) writeNull() else writeBoolean(src.getBoolean(srcIdx)) }
        }
}