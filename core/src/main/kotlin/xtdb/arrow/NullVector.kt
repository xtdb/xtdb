package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class NullVector(override val name: String) : Vector() {
    override var nullable: Boolean = true

    override val field: Field = Field(name, FieldType.nullable(ArrowType.Null.INSTANCE), emptyList())

    override fun isNull(idx: Int) = true

    override fun writeNull() {
        valueCount++
    }

    override fun getObject0(idx: Int) = error("NullVector getObject0")

    override fun writeObject0(value: Any) = error("NullVector writeObject0")

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), valueCount.toLong()))
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: error("missing node")
        valueCount = node.length
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = error("hashCode0 called on NullVector")

    override fun reset() {
        valueCount = 0
    }

    override fun close() {
    }

}
