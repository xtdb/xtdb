package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVectorHelper

internal val NULL_CHECKS =
    System.getenv("XTDB_VECTOR_NULL_CHECKS")?.toBoolean()
        ?: System.getProperty("xtdb.vector.null-checks")?.toBoolean()
        ?: true

internal class ExtensibleBuffer(private val allocator: BufferAllocator, private var buf: ArrowBuf) : AutoCloseable {

    constructor(allocator: BufferAllocator) : this(allocator, allocator.empty)

    private fun handleCapacity(capacity: Int) {
        val currentCapacity = buf.capacity()

        val newBuf = allocator.buffer(capacity.toLong()).apply {
            setBytes(0, buf, 0, currentCapacity)
            setZero(currentCapacity, capacity() - currentCapacity)
        }

        buf.close()
        buf = newBuf
    }

    private fun handleCapacity(elCount: Int, elWidth: Int) = handleCapacity(elCount * elWidth)

    fun getBit(idx: Int) = BitVectorHelper.get(buf, idx) == 1
    fun setBit(idx: Int, bit: Int) = BitVectorHelper.setValidityBit(buf, idx, bit)

    private fun setBitSafe(idx: Int, bit: Int) {
        handleCapacity(BitVectorHelper.getValidityBufferSize(idx + 1));
        setBit(idx, bit)
    }

    fun setBitSafe(idx: Int) = setBitSafe(idx, 1)
    fun unsetBitSafe(idx: Int) = setBitSafe(idx, 0)

    fun getInt(idx: Int) = buf.getInt((idx * Int.SIZE_BYTES).toLong())
    fun setInt(idx: Int, value: Int) = buf.setInt((idx * Int.SIZE_BYTES).toLong(), value)
    fun setIntSafe(idx: Int, value: Int) = apply { handleCapacity(idx + 1, Int.SIZE_BYTES) }.setInt(idx, value)

    override fun close() = buf.close()
}