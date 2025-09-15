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
    private val validityBuffer: ExtensibleBuffer, private val dataBuffer: ExtensibleBuffer
) : Vector(), MetadataFlavour.Presence {

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean
    ) : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val type: ArrowType = BIT_TYPE
    override val vectors: Iterable<Vector> = emptyList()

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    override fun writeUndefined() {
        validityBuffer.writeBit(valueCount, 0)
        dataBuffer.writeBit(valueCount++, 0)
    }

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    override fun getBoolean(idx: Int) =
        if (NULL_CHECKS && isNull(idx)) throw NullPointerException("null at index $idx") else dataBuffer.getBit(idx)

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
        return RowCopier { srcIdx ->
            if (src.nullable && !nullable) nullable = true
            valueCount.apply { if (src.isNull(srcIdx)) writeNull() else writeBoolean(src.getBoolean(srcIdx)) }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        dataBuffer.unloadBuffer(buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")
        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing validity buffer"))
        dataBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing data buffer"))

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowBitVector)
        validityBuffer.loadBuffer(vec.validityBuffer)
        dataBuffer.loadBuffer(vec.dataBuffer)

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