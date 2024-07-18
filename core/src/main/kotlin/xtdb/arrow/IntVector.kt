package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator

class IntVector(field: Field, allocator: BufferAllocator) : FixedWidthVector(field, allocator) {

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int) = getInt(idx)

    override fun writeObject0(value: Any) {
        if (value is Int) writeInt(value) else TODO("not an Int")
    }
}