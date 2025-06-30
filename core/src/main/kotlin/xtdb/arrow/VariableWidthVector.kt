package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.BaseVariableWidthVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import xtdb.util.Hasher
import java.nio.ByteBuffer

abstract class VariableWidthVector : Vector() {

    override val vectors: Iterable<Vector> = emptyList<Vector>()

    internal abstract val validityBuffer: ExtensibleBuffer
    internal abstract val offsetBuffer: ExtensibleBuffer
    internal abstract val dataBuffer: ExtensibleBuffer

    private var lastOffset: Int = 0

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    private fun writeOffset(newOffset: Int) {
        if (valueCount == 0) offsetBuffer.writeInt(0)
        offsetBuffer.writeInt(newOffset)
        lastOffset = newOffset
    }

    override fun writeUndefined() {
        writeOffset(lastOffset)
        validityBuffer.writeBit(valueCount++, 0)
    }

    private fun writeNotNull(len: Int) {
        writeOffset(lastOffset + len)
        validityBuffer.writeBit(valueCount++, 1)
    }

    override fun getBytes(idx: Int): ByteBuffer {
        val start = offsetBuffer.getInt(idx)
        val end = offsetBuffer.getInt(idx + 1)
        return dataBuffer.getBytes(start, end - start)
    }

    override fun writeBytes(v: ByteBuffer) {
        writeNotNull(v.remaining())
        dataBuffer.writeBytes(v.duplicate())
    }

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())

    override fun getPointer(idx: Int, reuse: ArrowBufPointer): ArrowBufPointer =
        offsetBuffer.getInt(idx).let { start ->
            dataBuffer.getPointer(start, offsetBuffer.getInt(idx + 1) - start, reuse)
        }

    protected fun getByteArray(idx: Int): ByteArray {
        val buf = getBytes(idx)
        return ByteArray(buf.remaining()).also { buf.duplicate().get(it) }
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getByteArray(idx))

    override fun rowCopier0(src: VectorReader): RowCopier {
        if (src.fieldType.type != type) throw InvalidCopySourceException(src.fieldType, fieldType)

        check(src is VariableWidthVector)
        nullable = nullable || src.nullable

        return RowCopier { srcIdx ->
            val start = src.offsetBuffer.getInt(srcIdx).toLong()
            val len = src.offsetBuffer.getInt(srcIdx + 1) - start
            dataBuffer.writeBytes(src.dataBuffer, start, len)
            valueCount.also { writeNotNull(len.toInt()) }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        offsetBuffer.unloadBuffer(buffers)
        dataBuffer.unloadBuffer(buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")

        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing validity buffer"))
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing offset buffer"))
        dataBuffer.loadBuffer(buffers.removeFirst() ?: error("missing data buffer"))

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is BaseVariableWidthVector)

        validityBuffer.loadBuffer(vec.validityBuffer)
        offsetBuffer.loadBuffer(vec.offsetBuffer)
        dataBuffer.loadBuffer(vec.dataBuffer)

        valueCount = vec.valueCount
    }

    override fun clear() {
        validityBuffer.clear()
        offsetBuffer.clear()
        dataBuffer.clear()
        valueCount = 0
        lastOffset = 0
    }

    override fun close() {
        validityBuffer.close()
        offsetBuffer.close()
        dataBuffer.close()
    }
}