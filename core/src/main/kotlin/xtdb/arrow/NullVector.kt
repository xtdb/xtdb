package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import org.apache.arrow.vector.NullVector as ArrowNullVector

internal val NULL_TYPE = ArrowType.Null.INSTANCE

class NullVector(override var name: String, override var valueCount: Int = 0) : Vector() {
    override val vectors = emptyList<Vector>()

    override var nullable: Boolean
        get() = true
        set(_) {}

    override val type: ArrowType = NULL_TYPE

    override fun isNull(idx: Int) = true

    override fun writeUndefined() {
        valueCount++
    }

    override fun writeNull() {
        valueCount++
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = error("NullVector getObject0")

    override fun writeObject0(value: Any) = throw InvalidWriteObjectException(fieldType, value)

    override fun writeValue0(v: ValueReader) = writeNull()

    override val metadataFlavours get() = emptyList<MetadataFlavour>()

    override fun hashCode0(idx: Int, hasher: Hasher) = error("hashCode0 called on NullVector")

    override fun rowCopier(dest: VectorWriter) =
        if (dest is DenseUnionVector) dest.rowCopier0(this)
        else {
            RowCopier {
                dest.valueCount.also { dest.writeNull() }
            }
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        return RowCopier { valueCount.also { writeNull() } }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), valueCount.toLong()))
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst()
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

    override fun openSlice(al: BufferAllocator) = NullVector(name)
}
