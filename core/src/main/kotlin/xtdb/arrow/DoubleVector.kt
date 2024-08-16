package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn

class DoubleVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(allocator, Double.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.FLOAT8.type

    override fun getDouble(idx: Int) = getDouble0(idx)
    override fun writeDouble(value: Double) = writeDouble0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getDouble(idx)

    override fun writeObject0(value: Any) {
        if (value is Double) writeDouble(value) else TODO("not a Double")
    }
}