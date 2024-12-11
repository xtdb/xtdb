package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import org.apache.arrow.vector.NullVector as ArrowNullVector

class NullVector(override val name: String) : Vector() {
    override val fieldType: FieldType = FieldType.nullable(ArrowType.Null.INSTANCE)
    override val children = emptyList<Vector>()

    override fun isNull(idx: Int) = true

    override fun writeNull() {
        valueCount++
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = error("NullVector getObject0")

    override fun writeObject0(value: Any) = error("NullVector writeObject0")

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = error("hashCode0 called on NullVector")

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is NullVector)
        return RowCopier { valueCount.also { writeNull() } }
    }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), valueCount.toLong()))
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: error("missing node")
        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowNullVector)

        valueCount = vec.valueCount
    }

    override fun clear() {
        valueCount = 0
    }

    override fun close() {
    }

}
