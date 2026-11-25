package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.InternalApi
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import xtdb.arrow.extensions.XtExtensionVector
import java.nio.ByteBuffer

sealed class ExtensionVector : MonoVector() {
    protected abstract val inner: MonoVector

    final override var nullable: Boolean
        get() = inner.nullable
        set(value) {
            inner.nullable = value
        }

    final override val vectors get() = inner.vectors
    override val childFields get() = inner.childFields

    override var valueCount: Int
        get() = inner.valueCount
        set(value) {
            inner.valueCount = value
        }

    override fun isNull(idx: Int) = inner.isNull(idx)
    override fun writeUndefined() = inner.writeUndefined()
    override fun writeNull() = inner.writeNull()

    override fun getBytes(idx: Int): ByteBuffer = inner.getBytes(idx)
    override fun writeBytes(v: ByteBuffer) = inner.writeBytes(v)

    override fun writeValue0(v: ValueReader) = inner.writeValue0(v)

    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = inner.getPointer(idx, reuse)

    override fun getListCount(idx: Int) = inner.getListCount(idx)
    override fun getListStartIndex(idx: Int) = inner.getListStartIndex(idx)
    override val listElements get() = inner.listElements

    override fun hashCode0(idx: Int, hasher: Hasher) = inner.hashCode0(idx, hasher)

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        inner.unloadPage(nodes, buffers)

    override fun unloadPage(target: Field, unloader: ConformingUnloader) =
        if (target.type == arrowType)
            inner.unloadPage(Field(target.name, FieldType(target.isNullable, inner.arrowType, null), target.children), unloader)
        else unloader.copy(this, target)

    @InternalApi
    override fun write(out: PageOutput, startIdx: Int, len: Int) = inner.write(out, startIdx, len)

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        inner.loadPage(nodes, buffers)

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is XtExtensionVector<*>)
        inner.loadFromArrow(vec.underlyingVector)
    }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is ExtensionVector)

        return inner.rowCopier0(src.inner)
    }

    override fun valueReader(): RowValueReader {
        val inner = inner.valueReader()

        return object : RowValueReader, ValueReader by inner {
            private val rowCount = valueCount
            override fun hasNext() = pos + 1 < rowCount

            override var pos: Int
                get() = inner.pos
                set(value) { inner.pos = value }

            override fun readObject() = readObject { it }
            override fun readObject(keyFn: IKeyFn<*>) = getObject(pos, keyFn)
        }
    }

    override fun clear() = inner.clear()
    override fun close() = inner.close()
}