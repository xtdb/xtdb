package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn

class LongVector(
    allocator: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : FixedWidthVector(allocator, nullable, MinorType.BIGINT.type, Long.SIZE_BYTES) {

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getLong(idx)

    override fun writeObject0(value: Any) {
        if (value is Long) writeLong(value) else TODO("not a Long")
    }
}