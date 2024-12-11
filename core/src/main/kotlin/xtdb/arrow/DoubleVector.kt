package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn

class DoubleVector(
    allocator: BufferAllocator,
    override val name: String,
    nullable: Boolean
) : FixedWidthVector(allocator, nullable, MinorType.FLOAT8.type, Double.SIZE_BYTES) {

    override fun getDouble(idx: Int) = getDouble0(idx)
    override fun writeDouble(value: Double) = writeDouble0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getDouble(idx)

    override fun writeObject0(value: Any) {
        if (value is Double) writeDouble(value) else TODO("not a Double")
    }
}