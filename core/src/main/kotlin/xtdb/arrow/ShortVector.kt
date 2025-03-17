package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher

class ShortVector(allocator: BufferAllocator, name: String, nullable: Boolean) :
    FixedWidthVector(allocator, name, nullable, MinorType.SMALLINT.type, Short.SIZE_BYTES) {

    override fun getShort(idx: Int) = getShort0(idx)
    override fun writeShort(value: Short) = writeShort0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getShort(idx)

    override fun writeObject0(value: Any) {
        if (value is Short) writeShort(value) else throw InvalidWriteObjectException(fieldType, value)
    }

   override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getShort(idx).toDouble())
}