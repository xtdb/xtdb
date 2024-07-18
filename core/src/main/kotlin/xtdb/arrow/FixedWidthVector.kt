package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator

sealed class FixedWidthVector(field: Field, allocator: BufferAllocator) : Vector(field) {
    override var valueCount: Int = 0

    private val validityBuffer = ExtensibleBuffer(allocator)
    private val dataBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)
    override fun writeNull() = validityBuffer.setBitSafe(valueCount++)
    private fun setNotNull(idx: Int) = validityBuffer.unsetBitSafe(idx)

    protected fun getInt0(idx: Int) = dataBuffer.getInt(idx)
    protected fun writeInt0(value: Int) {
        setNotNull(valueCount)
        dataBuffer.setIntSafe(valueCount++, value)
    }

    override fun close() {
        validityBuffer.close()
        dataBuffer.close()
    }
}
