package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn

class ShortVector(
    allocator: BufferAllocator,
    override val name: String,
    nullable: Boolean
) : FixedWidthVector(allocator, nullable, MinorType.SMALLINT.type, Short.SIZE_BYTES) {

    override fun getShort(idx: Int) = getShort0(idx)
    override fun writeShort(value: Short) = writeShort0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getShort(idx)

    override fun writeObject0(value: Any) {
        if (value is Short) writeShort(value) else TODO("not a Short")
    }
}