package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import java.nio.ByteBuffer

abstract class VariableWidthVector(allocator: BufferAllocator) : Vector() {

    override val arrowField: Field get() = Field(name, FieldType(nullable, arrowType, null), emptyList())
    abstract val arrowType: ArrowType

    private val validityBuffer = ExtensibleBuffer(allocator)
    private val offsetBuffer = ExtensibleBuffer(allocator)
    private val dataBuffer = ExtensibleBuffer(allocator)

    private var lastOffset: Int = 0

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    private fun writeOffset(newOffset: Int) {
        if (valueCount == 0) offsetBuffer.writeInt(0)
        offsetBuffer.writeInt(newOffset)
        lastOffset = newOffset
    }

    override fun writeNull() {
        writeOffset(lastOffset)
        validityBuffer.writeBit(valueCount++, 0)
    }

    private fun writeNotNull(len: Int) {
        writeOffset(lastOffset + len)
        validityBuffer.writeBit(valueCount++, 1)
    }

    override fun getBytes(idx: Int): ByteArray {
        val start = offsetBuffer.getInt(idx)
        val end = offsetBuffer.getInt(idx + 1)
        val res = ByteArray(end - start)
        return dataBuffer.getBytes(start, res)
    }

    override fun writeBytes(bytes: ByteArray) {
        writeNotNull(bytes.size)
        dataBuffer.writeBytes(bytes)
    }

    protected fun writeBytes(bytes: ByteBuffer) {
        writeNotNull(bytes.remaining())
        dataBuffer.writeBytes(bytes)
    }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        offsetBuffer.unloadBuffer(buffers)
        dataBuffer.unloadBuffer(buffers)
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")

        validityBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing validity buffer"))
        offsetBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing offset buffer"))
        dataBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing data buffer"))

        valueCount = node.length
    }

    override fun reset() {
        validityBuffer.reset()
        offsetBuffer.reset()
        dataBuffer.reset()
        valueCount = 0
        lastOffset = 0
    }

    override fun close() {
        validityBuffer.close()
        offsetBuffer.close()
        dataBuffer.close()
    }
}