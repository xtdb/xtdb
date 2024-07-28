package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import java.util.*

class StructVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
    private val children: SequencedMap<String, Vector> = LinkedHashMap()
) : Vector() {

    override val arrowField =
        Field(name, FieldType(nullable, ArrowType.Struct.INSTANCE, null), children.map { it.value.arrowField })

    private val validityBuffer = ExtensibleBuffer(allocator)

    private fun writeAbsents() {
        val valueCount = this.valueCount

        children.sequencedValues().forEach { child ->
            (child.valueCount until valueCount).forEach { _ -> child.writeNull() }
        }
    }

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    override fun writeNull() {
        validityBuffer.writeBit(valueCount++, 0)
        writeAbsents()
    }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)

        children.sequencedValues().forEach { it.unloadBatch(nodes, buffers) }
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")

        validityBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing validity buffer"))
        children.sequencedValues().forEach { it.loadBatch(nodes, buffers) }

        valueCount = node.length
    }

    override fun vectorForKey(name: String) = children[name]

    override fun endStruct() {
        validityBuffer.writeBit(valueCount++, 1)
        writeAbsents()
    }

    override fun getObject0(idx: Int): Any =
        children.sequencedEntrySet()
            .associateBy({ it.key }, { it.value.getObject(idx) })
            .filterValues { it != null }

    override fun writeObject0(value: Any) =
        if (value !is Map<*, *>) TODO("unknown type: ${value::class.simpleName}")
        else {
            value.forEach {
                (children[it.key] ?: TODO("promotion not supported yet")).writeObject(it.value)
            }
            endStruct()
        }

    override fun reset() {
        validityBuffer.reset()
        valueCount = 0
        children.sequencedValues().forEach(Vector::reset)
    }

    override fun close() {
        validityBuffer.close()
        children.sequencedValues().forEach(Vector::close)
    }
}