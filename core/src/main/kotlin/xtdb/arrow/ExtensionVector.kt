package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.vector.extensions.XtExtensionVector
import java.nio.ByteBuffer

abstract class ExtensionVector(private val arrowType: ArrowType) : Vector() {
    protected abstract val inner: Vector

    override var name
        get() = inner.name
        set(value) {
            inner.name = value
        }

    final override var fieldType
        get() = inner.fieldType.withArrowType(arrowType)
        set(value) {
            inner.fieldType = value.withArrowType(inner.fieldType.type)
        }

    final override val children get() = inner.children

    override var valueCount: Int
        get() = inner.valueCount
        set(value) {
            inner.valueCount = value
        }

    override fun isNull(idx: Int) = inner.isNull(idx)
    override fun writeNull() = inner.writeNull()

    override fun getBytes(idx: Int): ByteBuffer = inner.getBytes(idx)

    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = inner.getPointer(idx, reuse)

    override fun getListCount(idx: Int) = inner.getListCount(idx)
    override fun getListStartIndex(idx: Int) = inner.getListStartIndex(idx)
    override fun elementReader() = inner.elementReader()

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = inner.hashCode0(idx, hasher)

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        inner.unloadBatch(nodes, buffers)

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        inner.loadBatch(nodes, buffers)

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is XtExtensionVector<*>)
        inner.loadFromArrow(vec.underlyingVector)
    }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is ExtensionVector)
        return inner.rowCopier0(src.inner)
    }

    override fun clear() = inner.clear()
    override fun close() = inner.close()
}