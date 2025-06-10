package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.BaseFixedWidthVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.TimeUnit
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import java.nio.ByteBuffer

internal fun TimeUnit.toLong(seconds: Long, nanos: Int): Long = when (this) {
    TimeUnit.SECOND -> seconds
    TimeUnit.MILLISECOND -> seconds * 1000 + nanos / 1_000_000
    TimeUnit.MICROSECOND -> seconds * 1_000_000 + nanos / 1000
    TimeUnit.NANOSECOND -> seconds * 1_000_000_000 + nanos
}

sealed class FixedWidthVector : Vector() {

    protected abstract val byteWidth: Int
    override val vectors: Iterable<Vector> = emptyList()

    internal abstract val validityBuffer: ExtensibleBuffer
    internal abstract val dataBuffer: ExtensibleBuffer

    final override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    final override fun writeUndefined() {
        validityBuffer.writeBit(valueCount++, 0)
        dataBuffer.writeZero(byteWidth)
    }

    final override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    protected fun writeNotNull() = validityBuffer.writeBit(valueCount++, 1)

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

    override fun writeBytes(v: ByteBuffer) {
        dataBuffer.writeBytes(v)
        writeNotNull()
    }

    override fun getPointer(idx: Int, reuse: ArrowBufPointer) =
        dataBuffer.getPointer(idx * byteWidth, byteWidth, reuse)

    override val metadataFlavours get() = listOf(this as MetadataFlavour)

    override fun hashCode0(idx: Int, hasher: Hasher) =
        hasher.hash(getByteArray(idx))

    override fun rowCopier0(src: VectorReader): RowCopier {
        if (src.fieldType.type != type) throw InvalidCopySourceException(src.fieldType, fieldType)
        nullable = nullable || src.nullable

        check(src is FixedWidthVector)
        check(src.byteWidth == byteWidth)

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
        val node = nodes.removeFirstOrNull() ?: throw IllegalStateException("missing node")
        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing validity buffer"))
        dataBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing data buffer"))

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
