package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.BaseFixedWidthVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import xtdb.InternalApi
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorIndirection.Mask
import org.apache.arrow.vector.types.TimeUnit
import xtdb.arrow.VectorType.Scalar
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import java.nio.ByteBuffer

internal fun TimeUnit.toLong(seconds: Long, nanos: Int): Long = when (this) {
    TimeUnit.SECOND -> seconds
    TimeUnit.MILLISECOND -> seconds * 1000 + nanos / 1_000_000
    TimeUnit.MICROSECOND -> seconds * 1_000_000 + nanos / 1000
    TimeUnit.NANOSECOND -> seconds * 1_000_000_000 + nanos
}

sealed class FixedWidthVector : MonoVector() {

    protected abstract val al: BufferAllocator
    internal abstract val byteWidth: Int

    override val vectors: Iterable<Vector> = emptyList()
    override val monoType get() = Scalar(arrowType)

    internal abstract var validityBuffer: BitBuffer?
    internal abstract val dataBuffer: ExtensibleBuffer

    final override var nullable: Boolean
        get() = validityBuffer != null
        set(value) {
            if (value && validityBuffer == null)
                BitBuffer(al).also { validityBuffer = it }.writeOnes(valueCount)
        }

    final override fun isNull(idx: Int) = validityBuffer?.getBoolean(idx) == false

    final override fun valueReader(): RowValueReader = FixedWidthRows(this)

    override fun notNull(sel: IntArray?, mask: Mask): Mask = notNullBits(validityBuffer, valueCount, sel, mask)

    final override fun writeUndefined() {
        validityBuffer?.writeBit(valueCount, 0)
        dataBuffer.writeZero(byteWidth)
        valueCount++
    }

    final override fun writeNull() {
        nullable = true
        writeUndefined()
    }

    protected fun setNotNull(idx: Int) {
        validityBuffer?.setBit(idx, 1)
    }

    protected fun writeNotNull() {
        validityBuffer?.writeBit(valueCount, 1)
        valueCount++
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

    override fun ensureCapacity(valueCount: Int) {
        validityBuffer?.ensureCapacity(valueCount)
        dataBuffer.ensureCapacity((valueCount * byteWidth).toLong())
    }

    override fun setNull(idx: Int) {
        ensureCapacity(idx + 1)
        validityBuffer?.setBit(idx, 0)
        valueCount = maxOf(valueCount, idx + 1)
    }

    protected fun setByte0(idx: Int, v: Byte) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer[idx] = v
        valueCount = maxOf(valueCount, idx + 1)
    }

    protected fun setShort0(idx: Int, v: Short) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer[idx] = v
        valueCount = maxOf(valueCount, idx + 1)
    }

    protected fun setInt0(idx: Int, v: Int) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer[idx] = v
        valueCount = maxOf(valueCount, idx + 1)
    }

    protected fun setLong0(idx: Int, v: Long) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer[idx] = v
        valueCount = maxOf(valueCount, idx + 1)
    }

    protected fun setFloat0(idx: Int, v: Float) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer[idx] = v
        valueCount = maxOf(valueCount, idx + 1)
    }

    protected fun setDouble0(idx: Int, v: Double) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer[idx] = v
        valueCount = maxOf(valueCount, idx + 1)
    }

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

    protected fun writeBytes0(v: ByteBuffer) {
        dataBuffer.writeBytes(v)
        writeNotNull()
    }

    override fun getPointer(idx: Int, reuse: ArrowBufPointer) =
        dataBuffer.getPointer(idx * byteWidth, byteWidth, reuse)

    override val metadataFlavours get() = listOf(this as MetadataFlavour)

    override fun hashCode0(idx: Int, hasher: Hasher) =
        dataBuffer.hashCode(hasher, idx * byteWidth, byteWidth)

    override fun rowCopier0(src: VectorReader): RowCopier {
        nullable = nullable || src.nullable

        check(src is FixedWidthVector)
        check(src.byteWidth == byteWidth)

        return object : RowCopier {
            // duplicated here because accessing in the outer class was non-negligible in the profiler
            private val srcValidity = src.validityBuffer
            private val srcData = src.dataBuffer
            private val destValidity = this@FixedWidthVector.validityBuffer
            private val destData = this@FixedWidthVector.dataBuffer
            private val byteWidth = this@FixedWidthVector.byteWidth

            private fun ensureWriteable(len: Int) {
                destData.ensureWritable((len * byteWidth).toLong())
            }

            private fun unsafeCopyRange(srcIdx: Int, len: Int) {
                destData.unsafeWriteBytes(srcData, (srcIdx * byteWidth).toLong(), (len * byteWidth).toLong())
            }

            override fun copyRow(srcIdx: Int) {
                ensureWriteable(1)
                destValidity?.writeBoolean(srcValidity?.getBoolean(srcIdx) ?: true)
                unsafeCopyRange(srcIdx, 1)
                valueCount++
            }

            override fun copyRows(sel: IntArray) {
                ensureWriteable(sel.size)
                destValidity?.writeBits(srcValidity, sel)
                for (srcIdx in sel) unsafeCopyRange(srcIdx, 1)
                valueCount += sel.size
            }

            override fun copyRange(startIdx: Int, len: Int) {
                ensureWriteable(len)
                destValidity?.writeBits(srcValidity, startIdx, len)
                unsafeCopyRange(startIdx, len)
                valueCount += len
            }
        }
    }

    final override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), if (nullable) -1 else 0))
        if (nullable) validityBuffer?.unloadBuffer(buffers) else buffers.add(al.empty)
        dataBuffer.unloadBuffer(buffers)
    }

    @InternalApi
    final override fun write(out: PageOutput, startIdx: Int, len: Int) {
        out.writeNode(len, if (nullable) -1 else 0)
        if (nullable) validityBuffer?.writePage(out, startIdx, len) else out.writeEmptyBuffer()
        dataBuffer.writePage(out, startIdx.toLong() * byteWidth, len.toLong() * byteWidth)
    }

    final override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: throw IllegalStateException("missing node")
        valueCount = node.length

        val validityBuf = buffers.removeFirstOrNull() ?: throw IllegalStateException("missing validity buffer")
        validityBuffer?.loadBuffer(validityBuf, valueCount)
        dataBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing data buffer"))
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is BaseFixedWidthVector)
        validityBuffer?.loadBuffer(vec.validityBuffer, vec.valueCount)
        dataBuffer.loadBuffer(vec.dataBuffer)

        valueCount = vec.valueCount
    }

    final override fun clear() {
        validityBuffer?.clear()
        dataBuffer.clear()
        valueCount = 0
    }

    final override fun close() {
        validityBuffer?.close()
        dataBuffer.close()
    }
}

private class FixedWidthRows(private val vec: FixedWidthVector) :
    ValidityWordRows(vec.valueCount) {

    private val data = vec.dataBuffer
    override val validity get() = vec.validityBuffer

    private fun rawPos(): Int {
        if (NULL_CHECKS && isNull) throw NullPointerException("null at index $pos")
        return pos
    }

    override val leg get() = vec.getLeg(pos)

    override fun readBoolean() = vec.getBoolean(pos)
    override fun readByte() = data.getByte(rawPos())
    override fun readShort() = data.getShort(rawPos())
    override fun readInt() = data.getInt(rawPos())
    override fun readLong() = data.getLong(rawPos())
    override fun readFloat() = data.getFloat(rawPos())
    override fun readDouble() = data.getDouble(rawPos())
    override fun readAsLong() = vec.getAsLong(pos)
    override fun readAsDouble() = vec.getAsDouble(pos)
    override fun readBytes() = vec.getBytes(pos)
    override fun readObject(keyFn: IKeyFn<*>) = vec.getObject(pos, keyFn)
}
