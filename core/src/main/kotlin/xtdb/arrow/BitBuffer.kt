package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVectorHelper
import org.apache.arrow.vector.BitVectorHelper.byteIndex
import org.apache.arrow.vector.util.DataSizeRoundingUtil.divideBy8Ceil
import xtdb.arrow.VectorIndirection.Companion.Selection
import kotlin.math.max

internal class BitBuffer private constructor(
    private val allocator: BufferAllocator,
    var buf: ArrowBuf,
    var writerBitIndex: Int = 0
) : AutoCloseable {

    companion object {
        private fun bufferSize(bitCount: Int) = BitVectorHelper.getValidityBufferSize(bitCount).toLong()
    }

    constructor(allocator: BufferAllocator) : this(allocator, allocator.empty, 0)

    constructor(
        allocator: BufferAllocator, initialBitCapacity: Int
    ) : this(
        allocator,
        allocator.buffer(bufferSize(initialBitCapacity))
            .apply { setZero(0, capacity()) },
        0
    )

    private fun newCapacity(currentCapacity: Long, targetCapacity: Long): Long {
        var newCapacity = max(currentCapacity, 128)
        while (newCapacity < targetCapacity) newCapacity *= 2
        return newCapacity
    }

    private fun realloc(targetCapacityBytes: Long) {
        val currentCapacity = buf.capacity()

        val newBuf = allocator.buffer(newCapacity(currentCapacity, targetCapacityBytes)).apply {
            setBytes(0, buf, 0, currentCapacity)
            setZero(currentCapacity, capacity() - currentCapacity)
            readerIndex(buf.readerIndex())
            writerIndex(buf.writerIndex())
        }

        buf.close()
        buf = newBuf
    }

    fun ensureCapacity(bitCount: Int) {
        val capacity = bufferSize(bitCount)
        if (buf.capacity() < capacity) realloc(capacity)
    }

    fun ensureWritable(bitCount: Int) {
        ensureCapacity(writerBitIndex + bitCount)
    }

    fun getBoolean(idx: Int) = BitVectorHelper.get(buf, idx) == 1

    fun setBit(bitIdx: Int, bit: Int) = BitVectorHelper.setValidityBit(buf, bitIdx, bit)

    fun writeBit(bitIdx: Int, bit: Int) {
        ensureWritable(1)
        setBit(bitIdx, bit)
        writerBitIndex = bitIdx + 1
    }

    fun writeBit(bit: Int) {
        ensureWritable(1)
        setBit(writerBitIndex++, bit)
    }

    fun writeBoolean(bool: Boolean) = writeBit(if (bool) 1 else 0)

    private fun Int.isSetAtIndex(bitIdx: Int) = (this.shr(bitIdx % 8) and 1) == 1

    fun writeBits(src: BitBuffer, sel: VectorIndirection) {
        ensureWritable(sel.valueCount())
        val srcBuf = src.buf
        val destBuf = buf

        var writerBitIndex = this.writerBitIndex

        destBuf.writerIndex((writerBitIndex / 8).toLong())

        var tailByte = if (writerBitIndex % 8 == 0) 0 else destBuf.getByte(destBuf.writerIndex()).toInt()

        var cacheByte = -1
        var cacheByteIdx = -1L

        for (selIdx in sel) {
            val bitIdx = writerBitIndex % 8
            val mask = 1 shl bitIdx

            val selByteIdx = (selIdx / 8).toLong()
            if (selByteIdx != cacheByteIdx) {
                cacheByteIdx = selByteIdx
                cacheByte = srcBuf.getByte(selByteIdx).toInt()
            }

            tailByte = if (cacheByte.isSetAtIndex(selIdx)) tailByte or mask else tailByte and mask.inv()

            if (++writerBitIndex % 8 == 0) {
                destBuf.writeByte(tailByte)
                tailByte = 0
            }
        }

        // done - sync any temporary state

        this.writerBitIndex = writerBitIndex

        if (writerBitIndex % 8 != 0)
            destBuf.writeByte(tailByte)
    }

    internal fun unloadBuffer(buffers: MutableList<ArrowBuf>) {
        val writerByteIndex = bufferSize(writerBitIndex)
        buffers.add(buf.readerIndex(0).writerIndex(writerByteIndex))
    }

    internal fun loadBuffer(arrowBuf: ArrowBuf, bitCount: Int) {
        buf.close()
        buf = arrowBuf.writerIndex(bufferSize(bitCount))
            .let { it.referenceManager.transferOwnership(it, allocator).transferredBuffer }
        writerBitIndex = bitCount
    }

    fun openSlice(al: BufferAllocator) =
        BitBuffer(al, buf.referenceManager.transferOwnership(buf, al).transferredBuffer, writerBitIndex)

    fun clear() {
        buf.setZero(0, buf.capacity())
        buf.readerIndex(0)
        buf.writerIndex(0)
        writerBitIndex = 0
    }

    override fun close() {
        buf.close()
    }
}
