package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import org.apache.arrow.vector.BitVector as ArrowBitVector
import org.apache.arrow.vector.types.pojo.ArrowType.Bool.INSTANCE as BIT_TYPE

internal val BOOL_TYPE: ArrowType = ArrowType.Bool.INSTANCE

class BitVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    private val validityBuffer: BitBuffer, private val dataBuffer: BitBuffer
) : Vector(), MetadataFlavour.Presence {

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean
    ) : this(name, nullable, 0, BitBuffer(al), BitBuffer(al))

    override val arrowType: ArrowType = BIT_TYPE
    override val vectors: Iterable<Vector> = emptyList()

    override fun ensureCapacity(valueCount: Int) {
        this.valueCount = this.valueCount.coerceAtLeast(valueCount)
        validityBuffer.ensureCapacity(valueCount)
        dataBuffer.ensureCapacity(valueCount)
    }

    override fun isNull(idx: Int) = !validityBuffer.getBoolean(idx)

    override fun writeUndefined() {
        validityBuffer.writeBit(valueCount, 0)
        dataBuffer.writeBit(valueCount++, 0)
    }

    override fun setNull(idx: Int) {
        ensureCapacity(idx + 1)
        validityBuffer.setBit(idx, 0)
    }

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    override fun getBoolean(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx") else dataBuffer.getBoolean(idx)

    override fun setBoolean(idx: Int, v: Boolean) {
        ensureCapacity(idx + 1)

        validityBuffer.setBit(idx, 1)
        dataBuffer.setBit(idx, if (v) 1 else 0)
    }

    override fun writeBoolean(v: Boolean) {
        validityBuffer.writeBit(valueCount, 1)
        dataBuffer.writeBit(valueCount++, if (v) 1 else 0)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getBoolean(idx)

    override fun writeObject0(value: Any) {
        if (value is Boolean) writeBoolean(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeBoolean(v.readBoolean())

    override val metadataFlavours get() = listOf(this)

    override fun hashCode0(idx: Int, hasher: Hasher) = if (getBoolean(idx)) 17 else 19

    override fun rowCopier0(src: VectorReader): RowCopier {
        check(src is BitVector)
        val srcNullable = src.nullable

        return RowCopier { srcIdx ->
            if (srcNullable && !nullable) nullable = true
            if (src.isNull(srcIdx)) writeNull() else writeBoolean(src.getBoolean(srcIdx))
        }
    }

    override fun openUnloadedPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.openUnloadedBuffer(buffers)
        dataBuffer.openUnloadedBuffer(buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")
        valueCount = node.length

        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing validity buffer"), valueCount)
        dataBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing data buffer"), valueCount)
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowBitVector)
        validityBuffer.loadBuffer(vec.validityBuffer, vec.valueCount)
        dataBuffer.loadBuffer(vec.dataBuffer, vec.valueCount)

        valueCount = vec.valueCount
    }

    override fun clear() {
        validityBuffer.clear()
        dataBuffer.clear()
        valueCount = 0
    }

    override fun close() {
        validityBuffer.close()
        dataBuffer.close()
    }

    override fun openSlice(al: BufferAllocator) =
        BitVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}