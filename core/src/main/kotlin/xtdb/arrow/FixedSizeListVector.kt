package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class FixedSizeListVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
    private val listSize: Int,
    private val elVector: Vector
) : Vector() {

    override val field: Field
        get() = Field(name, FieldType(nullable, ArrowType.FixedSizeList(listSize), null), listOf(elVector.field))

    private val validityBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)
    override fun writeNull() = validityBuffer.writeBit(valueCount++, 0)
    private fun writeNotNull() = validityBuffer.writeBit(valueCount++, 1)

    override fun getObject0(idx: Int): List<*> {
        return (idx * listSize until (idx + 1) * listSize).map { elVector.getObject(it) }
    }

    override fun writeObject0(value: Any) = when (value) {
        is List<*> -> {
            require(value.size == listSize) { "invalid list size: expected $listSize, got ${value.size}" }
            writeNotNull()
            value.forEach { elVector.writeObject(it) }
        }

        else -> TODO("unknown type")
    }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        elVector.unloadBatch(nodes, buffers)
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")

        validityBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing validity buffer"))
        elVector.loadBatch(nodes, buffers)

        valueCount = node.length
    }

    override fun reset() {
        validityBuffer.reset()
        elVector.reset()
        valueCount = 0
    }

    override fun close() {
        validityBuffer.close()
        elVector.close()
    }
}