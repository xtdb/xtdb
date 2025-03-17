package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher

internal val I32_TYPE = MinorType.INT.type

class IntVector(allocator: BufferAllocator, name: String, nullable: Boolean) :
    FixedWidthVector(allocator, name, nullable, I32_TYPE, Int.SIZE_BYTES) {

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getInt(idx)

    override fun writeObject0(value: Any) {
        if (value is Int) writeInt(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getInt(idx).toDouble())
}