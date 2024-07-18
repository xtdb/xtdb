package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

sealed class FixedWidthVector(allocator: BufferAllocator) : Vector() {

    override val arrowField: Field get() = Field(name, FieldType(nullable, arrowType, null), emptyList())
    abstract val arrowType: ArrowType

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

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")
        validityBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing validity buffer"))
        dataBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing data buffer"))

        valueCount = node.length
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
