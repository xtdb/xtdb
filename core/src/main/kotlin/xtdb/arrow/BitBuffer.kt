package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVectorHelper
import org.apache.arrow.vector.BitVectorHelper.byteIndex
import org.apache.arrow.vector.util.DataSizeRoundingUtil.divideBy8Ceil
import kotlin.math.max

internal class BitBuffer private constructor(
    private val allocator: BufferAllocator,
    var buf: ArrowBuf,
    var writerBitIndex: Int = 0,
    private var bufferedByte: Int = 0,
    private var lastFlushedBitIndex: Int = 0
) : AutoCloseable {

    companion object {
        private fun bufferSize(bitCount: Int) = BitVectorHelper.getValidityBufferSize(bitCount).toLong()
    }

    constructor(allocator: BufferAllocator) : this(allocator, allocator.empty, 0, 0, 0)

    constructor(
        allocator: BufferAllocator, initialBitCapacity: Int
    ) : this(
        allocator,
        allocator.buffer(bufferSize(initialBitCapacity))
            .apply { setZero(0, capacity()) },
        0,
        0,
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

    fun getBit(idx: Int): Boolean {
        if (idx >= lastFlushedBitIndex && idx < writerBitIndex) {
            val bitOffset = idx - lastFlushedBitIndex
            return (bufferedByte and (1 shl bitOffset)) != 0
        }
        return BitVectorHelper.get(buf, idx) == 1
    }

    fun setBit(bitIdx: Int, bit: Int) = BitVectorHelper.setValidityBit(buf, bitIdx, bit)

    private fun flushBufferedBits() {
        val bufferedBitCount = writerBitIndex - lastFlushedBitIndex
        if (bufferedBitCount > 0) {
            val byteIdx = byteIndex(lastFlushedBitIndex.toLong()).toInt()
            val bitOffset = lastFlushedBitIndex % 8
            val currentByte = buf.getByte(byteIdx.toLong()).toInt()
            val mask = ((1 shl bufferedBitCount) - 1) shl bitOffset
            val newByte = (currentByte and mask.inv()) or ((bufferedByte shl bitOffset) and mask)
            buf.setByte(byteIdx.toLong(), newByte)
            bufferedByte = 0
            lastFlushedBitIndex = writerBitIndex
        }
    }

    fun writeBit(bitIdx: Int, bit: Int) {
        flushBufferedBits()
        ensureWritable(1)
        setBit(bitIdx, bit)
        writerBitIndex = bitIdx + 1
        lastFlushedBitIndex = writerBitIndex
    }

    fun writeBit(bit: Int) {
        ensureWritable(1)
        val bufferedBitCount = writerBitIndex - lastFlushedBitIndex
        bufferedByte = bufferedByte or (bit shl bufferedBitCount)
        writerBitIndex++
        if (bufferedBitCount == 7) {
            flushBufferedBits()
        }
    }

    fun unsafeWriteBits(src: BitBuffer, idx: Int, len: Int) {
        // Fast path: both byte-aligned, length is a multiple of 8, and source has no buffered bits
        if (writerBitIndex % 8 == 0 && idx % 8 == 0 && (len % 8 == 0 || writerBitIndex == 0) 
            && src.lastFlushedBitIndex == src.writerBitIndex) {
            val idxBytes = byteIndex(idx)
            val lenBytes = divideBy8Ceil(len)
            buf.setBytes(byteIndex(writerBitIndex.toLong()), src.buf, idxBytes.toLong(), lenBytes.toLong())
            writerBitIndex += len
            lastFlushedBitIndex = writerBitIndex
            return
        }

        // TODO medium path: copying bytes at a time. See BitVectorHelper.concatBits for inspiration.

        // Slow path: bit-by-bit copy
        flushBufferedBits()
        repeat(len) { setBit(writerBitIndex + it, if (src.getBit(idx + it)) 1 else 0) }
        writerBitIndex += len
        lastFlushedBitIndex = writerBitIndex
    }

    fun writeBits(src: BitBuffer, idx: Int, len: Int) {
        ensureWritable(len)
        unsafeWriteBits(src, idx, len)
    }

    internal fun unloadBuffer(buffers: MutableList<ArrowBuf>) {
        flushBufferedBits()
        val writerByteIndex = bufferSize(writerBitIndex)
        buffers.add(buf.readerIndex(0).writerIndex(writerByteIndex))
    }

    internal fun loadBuffer(arrowBuf: ArrowBuf, bitCount: Int) {
        buf.close()
        buf = arrowBuf.writerIndex(bufferSize(bitCount))
            .let { it.referenceManager.transferOwnership(it, allocator).transferredBuffer }
        writerBitIndex = bitCount
        bufferedByte = 0
        lastFlushedBitIndex = bitCount
    }

    fun openSlice(al: BufferAllocator): BitBuffer {
        flushBufferedBits()
        return BitBuffer(al, buf.referenceManager.transferOwnership(buf, al).transferredBuffer, writerBitIndex, 0, writerBitIndex)
    }

    fun clear() {
        buf.setZero(0, buf.capacity())
        buf.readerIndex(0)
        buf.writerIndex(0)
        writerBitIndex = 0
        bufferedByte = 0
        lastFlushedBitIndex = 0
    }

    override fun close() {
        buf.close()
    }
}
