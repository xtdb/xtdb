package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.BitVectorHelper
import org.apache.arrow.vector.util.DecimalUtility
import java.math.BigDecimal
import java.nio.ByteBuffer
import kotlin.math.max

internal val NULL_CHECKS =
    System.getenv("XTDB_VECTOR_NULL_CHECKS")?.toBoolean()
        ?: System.getProperty("xtdb.vector.null-checks")?.toBoolean()
        ?: true

internal class ExtensibleBuffer(private val allocator: BufferAllocator, private var buf: ArrowBuf) : AutoCloseable {

    constructor(allocator: BufferAllocator) : this(allocator, allocator.empty)
    constructor(allocator: BufferAllocator, initialSize: Long) : this(allocator, allocator.buffer(initialSize))

    private fun newCapacity(currentCapacity: Long, targetCapacity: Long): Long {
        var newCapacity = max(currentCapacity, 128)
        while (newCapacity < targetCapacity) newCapacity *= 2
        return newCapacity
    }

    private fun realloc(targetCapacity: Long) {
        val currentCapacity = buf.capacity()

        val newBuf = allocator.buffer(newCapacity(currentCapacity, targetCapacity)).apply {
            setBytes(0, buf, 0, currentCapacity)
            writerIndex(buf.writerIndex())
        }

        buf.close()
        buf = newBuf
    }

    private fun ensureWritable(elWidth: Long): ArrowBuf {
        while (buf.writableBytes() < elWidth) realloc(buf.writerIndex() + elWidth)
        return buf
    }

    private fun ensureCapacity(capacity: Long): ArrowBuf {
        while (buf.capacity() < capacity) realloc(capacity)
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

    fun writeZero(elWidth: Int) {
        ensureWritable(elWidth.toLong())
        val writerIndex = buf.writerIndex()
        buf.setZero(writerIndex, elWidth.toLong())
        buf.writerIndex(writerIndex + elWidth)
    }

    fun getByte(idx: Int) = buf.getByte((idx * Byte.SIZE_BYTES).toLong())

    fun writeByte(value: Byte) {
        ensureWritable(Byte.SIZE_BYTES.toLong())
        buf.writeByte(value.toInt())
    }

    fun getShort(idx: Int) = buf.getShort((idx * Short.SIZE_BYTES).toLong())

    fun writeShort(value: Short) {
        ensureWritable(Short.SIZE_BYTES.toLong())
        buf.writeShort(value.toInt())
    }

    fun getInt(idx: Int) = buf.getInt((idx * Int.SIZE_BYTES).toLong())

    operator fun set(idx: Int, v: Int) = buf.setInt(idx.toLong() * Int.SIZE_BYTES, v)

    fun writeInt(value: Int) {
        ensureWritable(Int.SIZE_BYTES.toLong())
        buf.writeInt(value)
    }

    fun getLong(idx: Int) = buf.getLong((idx * Long.SIZE_BYTES).toLong())

    fun writeLong(value: Long) {
        ensureWritable(Long.SIZE_BYTES.toLong())
        buf.writeLong(value)
    }

    fun getFloat(idx: Int) = buf.getFloat((idx * Float.SIZE_BYTES).toLong())

    fun writeFloat(value: Float) {
        ensureWritable(Float.SIZE_BYTES.toLong())
        buf.writeFloat(value)
    }

    fun getDouble(idx: Int) = buf.getDouble((idx * Double.SIZE_BYTES).toLong())

    fun writeDouble(value: Double) {
        ensureWritable(Double.SIZE_BYTES.toLong())
        buf.writeDouble(value)
    }

    fun getBytes(start: Int, len: Int): ByteBuffer = buf.nioBuffer(start.toLong(), len)

    fun writeBytes(bytes: ByteArray) {
        ensureWritable(bytes.size.toLong())
        buf.writeBytes(bytes)
    }

    fun writeBytes(bytes: ByteBuffer) {
        val bytesDup = bytes.duplicate()
        ensureWritable(bytesDup.remaining().toLong())
        val byteArray = ByteArray(bytesDup.remaining())
        bytesDup.get(byteArray)
        buf.writeBytes(byteArray)
    }

    fun writeBytes(src: ExtensibleBuffer, start: Long, len: Long) {
        ensureWritable(len)
        val writerIndex = buf.writerIndex()
        buf.setBytes(writerIndex, src.buf, start, len)
        buf.writerIndex(writerIndex + len)
    }

    fun writeBigDecimal(value: BigDecimal, byteWidth: Int) {
        ensureWritable(byteWidth.toLong())
        DecimalUtility.writeBigDecimalToArrowBuf(value, buf, (buf.writerIndex() / byteWidth).toInt(), byteWidth)
        buf.writerIndex(buf.writerIndex() + byteWidth)
    }

    fun readBigDecimal(idx: Int, scale: Int, byteWidth: Int) =
        DecimalUtility.getBigDecimalFromArrowBuf(buf, idx, scale, byteWidth)

    fun getPointer(idx: Int, len: Int, reuse: ArrowBufPointer? = null) =
        (reuse ?: ArrowBufPointer()).apply { set(this@ExtensibleBuffer.buf, idx.toLong(), len.toLong()) }

    internal fun unloadBuffer(buffers: MutableList<ArrowBuf>) = buffers.add(buf.readerIndex(0))

    internal fun loadBuffer(arrowBuf: ArrowBuf, writerIndex: Long = arrowBuf.writerIndex()) {
        buf.close()
        buf = arrowBuf.writerIndex(writerIndex)
            .let { it.referenceManager.transferOwnership(it, allocator).transferredBuffer }
    }

    fun openSlice(al: BufferAllocator) =
        ExtensibleBuffer(al, buf.referenceManager.transferOwnership(buf, al).transferredBuffer)

    fun clear() {
        buf.setZero(0, buf.capacity())
        buf.readerIndex(0)
        buf.writerIndex(0)
    }

    override fun close() {
        buf.close()
    }

    fun hashCode(hasher: ArrowBufHasher, start: Long, len: Long) = hasher.hashCode(buf, start, len)
}
