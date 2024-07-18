package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVectorHelper
import kotlin.math.max

internal val NULL_CHECKS =
    System.getenv("XTDB_VECTOR_NULL_CHECKS")?.toBoolean()
        ?: System.getProperty("xtdb.vector.null-checks")?.toBoolean()
        ?: true

internal class ExtensibleBuffer(private val allocator: BufferAllocator, private var buf: ArrowBuf) : AutoCloseable {

    constructor(allocator: BufferAllocator) : this(allocator, allocator.empty)

    private fun realloc() {
        val currentCapacity = buf.capacity()
        val newBuf = allocator.buffer(max(128, currentCapacity * 2)).apply {
            setBytes(0, buf, 0, currentCapacity)
            writerIndex(buf.writerIndex())
        }

        buf.close()
        buf = newBuf
    }

    private fun ensureWritable(elWidth: Long): ArrowBuf {
        if (buf.writableBytes() < elWidth) realloc()
        return buf
    }

    private fun ensureCapacity(capacity: Long): ArrowBuf {
        if (buf.capacity() < capacity) realloc()
        return buf
    }

    fun getBit(idx: Int) = BitVectorHelper.get(buf, idx) == 1
    fun setBit(bitIdx: Int, bit: Int) = BitVectorHelper.setValidityBit(buf, bitIdx, bit)

    fun writeBit(bitIdx: Int, bit: Int) {
        val validityBufferSize = BitVectorHelper.getValidityBufferSize(bitIdx + 1)
        ensureCapacity(validityBufferSize.toLong())
        setBit(bitIdx, bit)
        buf.writerIndex(validityBufferSize.toLong())
    }

    fun getInt(idx: Int) = buf.getInt((idx * Int.SIZE_BYTES).toLong())

    fun writeInt(value: Int) {
        ensureWritable(Int.SIZE_BYTES.toLong())
        buf.writeInt(value)
    }

    fun getBytes(start: Int, out: ByteArray): ByteArray {
        buf.getBytes(start.toLong(), out)
        return out
    }

    fun writeBytes(bytes: ByteArray) {
        ensureWritable(bytes.size.toLong())
        buf.writeBytes(bytes)
    }

    internal fun unloadBuffer(buffers: MutableList<ArrowBuf>) = buffers.add(buf.readerIndex(0))

    internal fun loadBuffer(arrowBuf: ArrowBuf) {
        buf.close()
        buf = arrowBuf.also { it.referenceManager.retain() }
    }

    fun reset() {
        buf.setZero(0, buf.capacity())
        buf.readerIndex(0)
        buf.writerIndex(0)
    }

    override fun close() {
        buf.close()
    }


}