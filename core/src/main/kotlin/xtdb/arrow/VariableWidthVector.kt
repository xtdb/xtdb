package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.BaseVariableWidthVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import xtdb.InternalApi
import xtdb.arrow.VectorIndirection.Companion.Selection
import xtdb.arrow.VectorIndirection.Companion.Slice
import xtdb.arrow.VectorType.Scalar
import xtdb.util.Hasher
import java.nio.ByteBuffer

abstract class VariableWidthVector : MonoVector() {

    override val vectors: Iterable<Vector> = emptyList()
    override val monoType get() = Scalar(arrowType)

    internal abstract val al: BufferAllocator
    internal abstract var validityBuffer: BitBuffer?
    internal abstract val offsetBuffer: ExtensibleBuffer
    internal abstract val dataBuffer: ExtensibleBuffer

    override var nullable: Boolean
        get() = validityBuffer != null
        set(value) {
            if (value && validityBuffer == null)
                BitBuffer(al).also { validityBuffer = it }.writeOnes(valueCount)
        }

    /**
     * The end offset of the last value written — held rather than read back, because this is the per-value
     * write path.
     *
     * Abstract so that it is seeded where [offsetBuffer] is: a vector constructed over buffers that already
     * hold values, from [openSlice] or a loaded page, has to continue their offsets, where starting at zero
     * gives every value it goes on to write an end offset below its own start.
     */
    protected abstract var lastOffset: Int

    override fun isNull(idx: Int) = nullable && !validityBuffer!!.getBoolean(idx)

    private fun writeOffset(newOffset: Int) {
        if (valueCount == 0) offsetBuffer.writeInt(0)
        offsetBuffer.writeInt(newOffset)
        lastOffset = newOffset
    }

    override fun writeUndefined() {
        writeOffset(lastOffset)
        validityBuffer?.writeBit(valueCount, 0)
        valueCount++
    }

    private fun writeNotNull(len: Int) {
        writeOffset(lastOffset + len)
        validityBuffer?.writeBit(valueCount, 1)
        valueCount++
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

    override fun writeBytes(v: ByteArray) {
        writeNotNull(v.size)
        dataBuffer.writeBytes(v)
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
                destValidity?.ensureWritable(len)
                destOffset.ensureWritable(((len + 1) * Integer.BYTES).toLong())
                destData.ensureWritable(dataBytes.toLong())
            }

            private fun unsafeCopyRange(startIdx: Int, len: Int) {
                // offsets
                if (valueCount == 0) destOffset.unsafeWriteInt(0)

                val srcStartOffset = srcOffset.getInt(startIdx)
                val srcEndOffset = srcOffset.getInt(startIdx + len)
                val offsetDelta = lastOffset - srcStartOffset

                repeat(len) {
                    val offset = srcOffset.getInt(startIdx + it + 1) + offsetDelta
                    destOffset.unsafeWriteInt(offset)
                }

                // data
                val dataLen = srcEndOffset - srcStartOffset
                destData.unsafeWriteBytes(srcData, srcStartOffset.toLong(), dataLen.toLong())
                lastOffset += dataLen

                valueCount += len
            }

            override fun copyRow(srcIdx: Int) {
                ensureWriteable(1, srcOffset.getInt(srcIdx + 1) - srcOffset.getInt(srcIdx))
                destValidity?.writeBits(srcValidity, Slice(srcIdx, 1))

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

                destValidity?.writeBits(srcValidity, Selection(sel))

                for (srcIdx in sel)
                    unsafeCopyRange(srcIdx, 1)
            }

            override fun copyRange(startIdx: Int, len: Int) {
                if (len <= 0) return

                val totalDataBytes = srcOffset.getInt(startIdx + len) - srcOffset.getInt(startIdx)

                ensureWriteable(len, totalDataBytes)

                destValidity?.writeBits(srcValidity, Slice(startIdx, len))

                unsafeCopyRange(startIdx, len)
            }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), if (nullable) -1 else 0))
        if (nullable) validityBuffer?.unloadBuffer(buffers) else buffers.add(al.empty)
        offsetBuffer.unloadBuffer(buffers)
        dataBuffer.unloadBuffer(buffers)
    }

    @InternalApi
    override fun write(out: PageOutput, startIdx: Int, len: Int) {
        out.writeNode(len, if (nullable) -1 else 0)
        if (nullable) validityBuffer?.writePage(out, startIdx, len) else out.writeEmptyBuffer()

        if (valueCount == 0) {
            // nothing has been written, so there isn't even the leading zero offset to rebase from
            offsetBuffer.writePage(out)
            dataBuffer.writePage(out)
        } else {
            val dataStart = offsetBuffer.getInt(startIdx)
            val dataLen = offsetBuffer.getInt(startIdx + len) - dataStart
            offsetBuffer.writeRebasedOffsets(out, startIdx, len)
            dataBuffer.writePage(out, dataStart.toLong(), dataLen.toLong())
        }
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")
        valueCount = node.length

        val validityBuf = buffers.removeFirstOrNull() ?: error("missing validity buffer")
        validityBuffer?.loadBuffer(validityBuf, valueCount)
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing offset buffer"))
        dataBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing data buffer"))
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is BaseVariableWidthVector)

        validityBuffer?.loadBuffer(vec.validityBuffer, vec.valueCount)
        offsetBuffer.loadBuffer(vec.offsetBuffer)
        dataBuffer.loadBuffer(vec.dataBuffer)

        valueCount = vec.valueCount
    }

    override fun clear() {
        validityBuffer?.clear()
        offsetBuffer.clear()
        dataBuffer.clear()
        valueCount = 0
        lastOffset = 0
    }

    override fun close() {
        validityBuffer?.close()
        offsetBuffer.close()
        dataBuffer.close()
    }
}