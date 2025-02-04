package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.BaseFixedWidthVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import java.nio.ByteBuffer

internal fun TimeUnit.toLong(seconds: Long, nanos: Int): Long = when (this) {
    TimeUnit.SECOND -> seconds
    TimeUnit.MILLISECOND -> seconds * 1000 + nanos / 1_000_000
    TimeUnit.MICROSECOND -> seconds * 1_000_000 + nanos / 1000
    TimeUnit.NANOSECOND -> seconds * 1_000_000_000 + nanos
}

sealed class FixedWidthVector(
    allocator: BufferAllocator,
    nullable: Boolean,
    arrowType: ArrowType,
    val byteWidth: Int
) : Vector() {

    final override var fieldType = FieldType(nullable, arrowType, null)
    override val children: Iterable<Vector> = emptyList()

    private val validityBuffer = ExtensibleBuffer(allocator)
    private val dataBuffer = ExtensibleBuffer(allocator)

    final override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    final override fun writeUndefined() {
        if (byteWidth == 0) dataBuffer.writeBit(valueCount, 0) else dataBuffer.writeZero(byteWidth)
        validityBuffer.writeBit(valueCount++, 0)
    }

    final override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    private fun writeNotNull() = validityBuffer.writeBit(valueCount++, 1)

    protected fun getBoolean0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getBit(idx)

    protected fun writeBoolean0(value: Boolean) {
        dataBuffer.writeBit(valueCount, if (value) 1 else 0)
        writeNotNull()
    }

    protected fun getByte0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getByte(idx)

    protected fun writeByte0(value: Byte) {
        dataBuffer.writeByte(value)
        writeNotNull()
    }

    protected fun getShort0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getShort(idx)

    protected fun writeShort0(value: Short) {
        dataBuffer.writeShort(value)
        writeNotNull()
    }

    protected fun getInt0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getInt(idx)

    protected fun writeInt0(value: Int) {
        dataBuffer.writeInt(value)
        writeNotNull()
    }

    protected fun getLong0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getLong(idx)

    protected fun writeLong0(value: Long) {
        dataBuffer.writeLong(value)
        writeNotNull()
    }

    protected fun getFloat0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getFloat(idx)

    protected fun writeFloat0(value: Float) {
        dataBuffer.writeFloat(value)
        writeNotNull()
    }

    protected fun getDouble0(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx")
        else dataBuffer.getDouble(idx)

    protected fun writeDouble0(value: Double) {
        dataBuffer.writeDouble(value)
        writeNotNull()
    }

    protected fun getBytes0(idx: Int) = dataBuffer.getBytes(idx * byteWidth, byteWidth)

    protected fun getByteArray(idx: Int): ByteArray {
        val buf = getBytes0(idx)
        return ByteArray(buf.remaining()).also { buf.duplicate().get(it) }
    }

    override fun writeBytes(buf: ByteBuffer) {
        dataBuffer.writeBytes(buf)
        writeNotNull()
    }

    override fun getPointer(idx: Int, reuse: ArrowBufPointer) =
        dataBuffer.getPointer(idx * byteWidth, byteWidth, reuse)

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) =
        dataBuffer.hashCode(hasher, (idx * byteWidth).toLong(), byteWidth.toLong())

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is FixedWidthVector)
        require(src.byteWidth == byteWidth)
        return RowCopier { srcIdx ->
            dataBuffer.writeBytes(src.dataBuffer, (srcIdx * byteWidth).toLong(), byteWidth.toLong())
            valueCount.also { writeNotNull() }
        }
    }

    final override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        dataBuffer.unloadBuffer(buffers)
    }

    final override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")
        validityBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing validity buffer"))
        dataBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing data buffer"))

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is BaseFixedWidthVector)
        validityBuffer.loadBuffer(vec.validityBuffer)
        dataBuffer.loadBuffer(vec.dataBuffer)

        valueCount = vec.valueCount
    }

    final override fun clear() {
        validityBuffer.clear()
        dataBuffer.clear()
        valueCount = 0
    }

    final override fun close() {
        validityBuffer.close()
        dataBuffer.close()
    }
}
