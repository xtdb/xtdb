package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode

sealed class FixedWidthVector(field: Field, allocator: BufferAllocator) : Vector(field) {
    override var valueCount: Int = 0

    private val validityBuffer = ExtensibleBuffer(allocator)
    private val dataBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)
    override fun writeNull() = validityBuffer.writeBit(valueCount++, 0)
    private fun writeNotNull() = validityBuffer.writeBit(valueCount++, 1)

    protected fun getInt0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getInt(idx)

    protected fun writeInt0(value: Int) {
        dataBuffer.writeInt(value)
        writeNotNull()
    }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        dataBuffer.unloadBuffer(buffers)
    }

    override fun reset() {
        validityBuffer.reset()
        dataBuffer.reset()
        valueCount = 0
    }

    override fun close() {
        validityBuffer.close()
        dataBuffer.close()
    }
}
