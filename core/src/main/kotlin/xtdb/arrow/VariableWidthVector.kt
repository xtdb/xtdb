package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.BaseVariableWidthVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import xtdb.util.Hasher
import java.nio.ByteBuffer

abstract class VariableWidthVector : Vector() {

    override val vectors: Iterable<Vector> = emptyList()

    internal abstract val validityBuffer: BitBuffer
    internal abstract val offsetBuffer: ExtensibleBuffer
    internal abstract val dataBuffer: ExtensibleBuffer

    private var lastOffset: Int = 0

    override fun isNull(idx: Int) = nullable && !validityBuffer.getBoolean(idx)

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

    override fun hashCode0(idx: Int, hasher: Hasher): Int {
        val start = offsetBuffer.getInt(idx)
        val end = offsetBuffer.getInt(idx + 1)
        return dataBuffer.hashCode(hasher, start, end - start)
    }

    override fun rowCopier0(src: VectorReader): RowCopier {
        check(src is VariableWidthVector)
        nullable = nullable || src.nullable

        return object : RowCopier {
            // duplicated here because accessing in the outer class was non-negligible in the profiler
            private val srcValidity = src.validityBuffer
            private val srcOffset = src.offsetBuffer
            private val srcData = src.dataBuffer
            private val destValidity = this@VariableWidthVector.validityBuffer
            private val destOffset = this@VariableWidthVector.offsetBuffer
            private val destData = this@VariableWidthVector.dataBuffer

            private fun ensureWriteable(len: Int, dataBytes: Int) {
                destValidity.ensureWritable(len)
                destOffset.ensureWritable(((len + 1) * Integer.BYTES).toLong())
                destData.ensureWritable(dataBytes.toLong())
            }

            private fun unsafeCopyRange(startIdx: Int, len: Int) {
                destValidity.unsafeWriteBits(srcValidity, startIdx, len)

                val srcStartOffset = srcOffset.getInt(startIdx)
                val srcEndOffset = srcOffset.getInt(startIdx + len)
                val dataLen = srcEndOffset - srcStartOffset
                destData.unsafeWriteBytes(srcData, srcStartOffset.toLong(), dataLen.toLong())

                if (valueCount == 0) destOffset.unsafeWriteInt(0)
                val offsetDelta = lastOffset - srcStartOffset

                for (i in 0 until len) {
                    val offset = srcOffset.getInt(startIdx + i + 1) + offsetDelta
                    destOffset.unsafeWriteInt(offset)
                }

                lastOffset += dataLen
                valueCount += len
            }

            override fun copyRow(srcIdx: Int) {
                ensureWriteable(1, srcOffset.getInt(srcIdx + 1) - srcOffset.getInt(srcIdx))
                unsafeCopyRange(srcIdx, 1)
            }

            override fun copyRows(sel: IntArray) {
                var totalDataBytes = 0
                for (srcIdx in sel) {
                    val startOffset = srcOffset.getInt(srcIdx)
                    val endOffset = srcOffset.getInt(srcIdx + 1)
                    totalDataBytes += endOffset - startOffset
                }

                ensureWriteable(sel.size, totalDataBytes)

                for (srcIdx in sel)
                    unsafeCopyRange(srcIdx, 1)
            }

            override fun copyRange(startIdx: Int, len: Int) {
                if (len <= 0) return

                val totalDataBytes = srcOffset.getInt(startIdx + len) - srcOffset.getInt(startIdx)

                ensureWriteable(len, totalDataBytes)
                unsafeCopyRange(startIdx, len)
            }
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
        valueCount = node.length

        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing validity buffer"), valueCount)
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing offset buffer"))
        dataBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing data buffer"))
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is BaseVariableWidthVector)

        validityBuffer.loadBuffer(vec.validityBuffer, vec.valueCount)
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