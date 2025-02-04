package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import org.apache.arrow.vector.NullVector as ArrowNullVector

internal val NULL_TYPE = ArrowType.Null.INSTANCE

class NullVector(override var name: String) : Vector() {
    override var fieldType: FieldType = FieldType.nullable(NULL_TYPE)
    override val children = emptyList<Vector>()

    override fun isNull(idx: Int) = true

    override fun writeUndefined() {
        valueCount++
    }

    override fun writeNull() {
        valueCount++
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = error("NullVector getObject0")

    override fun writeObject0(value: Any) = throw InvalidWriteObjectException(fieldType, value)

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = error("hashCode0 called on NullVector")

    override fun rowCopier(dest: VectorWriter) =
        if (dest is DenseUnionVector) dest.rowCopier0(this)
        else {
            require(dest.nullable)
            RowCopier {
                dest.valueCount.also { dest.writeNull() }
            }
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is NullVector)
        return RowCopier { valueCount.also { writeNull() } }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), valueCount.toLong()))
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
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
