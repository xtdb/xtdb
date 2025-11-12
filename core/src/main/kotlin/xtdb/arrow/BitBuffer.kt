package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVectorHelper
import xtdb.util.closeOnCatch
import kotlin.math.max

internal class BitBuffer private constructor(
    private val allocator: BufferAllocator,
    var buf: ArrowBuf,
    var writerBitIndex: Int = 0
) : AutoCloseable {

    companion object {
        private fun bufferSize(bitCount: Int) = BitVectorHelper.getValidityBufferSize(bitCount).toLong()

        fun openAllOnes(al: BufferAllocator, bitCount: Int): ArrowBuf =
            al.buffer(bufferSize(bitCount)).closeOnCatch {
                it.setOne(0, it.capacity())
                it.writerIndex(it.capacity())
            }
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

    fun writeOnes(len: Int) {
        ensureWritable(len)

        var bitsRemaining = len
        val destBuf = buf
        var writerBitIndex = this.writerBitIndex

        // Handle partial first byte if not aligned
        if (writerBitIndex % 8 != 0) {
            val bitsInFirstByte = minOf(8 - (writerBitIndex % 8), bitsRemaining)
            var tailByte = destBuf.getByte((writerBitIndex / 8).toLong()).toInt()

            for (i in 0 until bitsInFirstByte) {
                val bitIdx = (writerBitIndex + i) % 8
                tailByte = tailByte or (1 shl bitIdx)
            }

            destBuf.setByte((writerBitIndex / 8).toLong(), tailByte)
            writerBitIndex += bitsInFirstByte
            bitsRemaining -= bitsInFirstByte
        }

        val fullBytes = bitsRemaining / 8
        if (fullBytes > 0) {
            val byteOffset = (writerBitIndex / 8).toLong()
            destBuf.setOne(byteOffset, fullBytes.toLong())
            writerBitIndex += fullBytes * 8
            bitsRemaining -= fullBytes * 8
        }

        if (bitsRemaining > 0) {
            var tailByte = 0
            for (i in 0 until bitsRemaining) {
                tailByte = tailByte or (1 shl i)
            }
            destBuf.setByte((writerBitIndex / 8).toLong(), tailByte)
            writerBitIndex += bitsRemaining
        }

        this.writerBitIndex = writerBitIndex
    }

    fun writeBits(src: BitBuffer?, sel: VectorIndirection) {
        if (src == null) {
            writeOnes(sel.valueCount())
            return
        }

        ensureWritable(sel.valueCount())

        val srcRdr = src.valueReader()
        val destBuf = buf

        var writerBitIndex = this.writerBitIndex

        destBuf.writerIndex((writerBitIndex / 8).toLong())

        var tailByte = if (writerBitIndex % 8 == 0) 0 else destBuf.getByte(destBuf.writerIndex()).toInt()

        for (selIdx in sel) {
            val bitIdx = writerBitIndex % 8
            val mask = 1 shl bitIdx
            srcRdr.pos = selIdx

            tailByte = if (srcRdr.readBoolean()) tailByte or mask else tailByte and mask.inv()

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

    fun valueReader() = object : ValueReader {
        override var pos = 0

        private var cacheByte = -1
        private var cacheByteIdx = -1L

        private fun Int.isSetAtIndex(bitIdx: Int) = (this.shr(bitIdx % 8) and 1) == 1

        override fun readBoolean(): Boolean {
            val pos = this.pos
            val selByteIdx = (pos / 8).toLong()
            if (selByteIdx != cacheByteIdx) {
                cacheByteIdx = selByteIdx
                cacheByte = buf.getByte(selByteIdx).toInt()
            }

            return cacheByte.isSetAtIndex(pos)
        }
    }

    internal fun openUnloadedBuffer(buffers: MutableList<ArrowBuf>) {
        val writerByteIndex = bufferSize(writerBitIndex)
        buffers.add(buf.readerIndex(0).writerIndex(writerByteIndex).also { it.referenceManager.retain() })
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
