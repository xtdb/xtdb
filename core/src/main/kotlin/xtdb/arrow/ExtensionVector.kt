package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ipc.message.ArrowFieldNode

abstract class ExtensionVector : Vector() {
    protected abstract val inner: Vector

    override val name get() = inner.name

    override var nullable: Boolean
        get() = inner.nullable
        set(value) {inner.nullable = value}

    override fun isNull(idx: Int) = inner.isNull(idx)
    override fun writeNull() = inner.writeNull()

    override fun getBytes(idx: Int) = inner.getBytes(idx)

    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = inner.getPointer(idx, reuse)

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = inner.hashCode0(idx, hasher)

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        inner.unloadBatch(nodes, buffers)

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        inner.loadBatch(nodes, buffers)

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is ExtensionVector)
        return inner.rowCopier0(src.inner)
    }

    override fun reset() = inner.reset()
    override fun close() = inner.close()
}