package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class DenseUnionVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
    legs: List<Vector>
) : Vector() {

    inner class LegVector(private val typeId: Byte, val inner: Vector) : Vector() {

        override val name get() = inner.name

        override var nullable: Boolean
            get() = inner.nullable
            set(value) {
                inner.nullable = value
            }

        override var valueCount
            get() = inner.valueCount
            set(value) {
                inner.valueCount = value
            }

        override val arrowField get() = inner.arrowField

        private fun writeValueThen(): Vector {
            typeBuffer.writeByte(typeId)
            offsetBuffer.writeInt(inner.valueCount)
            this@DenseUnionVector.valueCount++
            return inner
        }

        override fun isNull(idx: Int) = inner.isNull(getOffset(idx))
        override fun writeNull() = writeValueThen().writeNull()

        override fun getByte(idx: Int) = inner.getByte(getOffset(idx))
        override fun writeByte(value: Byte) = writeValueThen().writeByte(value)

        override fun getShort(idx: Int) = inner.getShort(getOffset(idx))
        override fun writeShort(value: Short) = writeValueThen().writeShort(value)

        override fun getInt(idx: Int) = inner.getInt(getOffset(idx))
        override fun writeInt(value: Int) = writeValueThen().writeInt(value)

        override fun getLong(idx: Int) = inner.getLong(getOffset(idx))
        override fun writeLong(value: Long) = writeValueThen().writeLong(value)

        override fun getFloat(idx: Int) = inner.getFloat(getOffset(idx))
        override fun writeFloat(value: Float) = writeValueThen().writeFloat(value)

        override fun getDouble(idx: Int) = inner.getDouble(getOffset(idx))
        override fun writeDouble(value: Double) = writeValueThen().writeDouble(value)

        override fun getBytes(idx: Int) = inner.getBytes(getOffset(idx))
        override fun writeBytes(bytes: ByteArray) = writeValueThen().writeBytes(bytes)

        override fun getObject(idx: Int) = inner.getObject(getOffset(idx))
        override fun getObject0(idx: Int) = throw UnsupportedOperationException()
        override fun writeObject0(value: Any) = writeValueThen().writeObject(value)

        override fun toList() = inner.toList()

        override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
            inner.unloadBatch(nodes, buffers)
        }

        override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
            inner.loadBatch(nodes, buffers)
        }

        override fun reset() {
            inner.reset()
        }

        override fun close() {
            inner.close()
        }
    }

    private val legs = legs.mapIndexed { idx, leg -> LegVector(idx.toByte(), leg)}
    operator fun get(typeId: Byte) = legs[typeId.toInt()]
    operator fun get(leg: String) = legs.find { it.name == leg }

    override val arrowField = Field(name, FieldType.notNullable(MinorType.DENSEUNION.type), legs.map { it.arrowField })

    private val typeBuffer = ExtensibleBuffer(allocator)
    fun getTypeId(idx: Int) = typeBuffer.getByte(idx)
    internal fun typeIds() = (0 until valueCount).map { typeBuffer.getByte(it) }

    private val offsetBuffer = ExtensibleBuffer(allocator)
    fun getOffset(idx: Int) = offsetBuffer.getInt(idx)
    internal fun offsets() = (0 until valueCount).map { offsetBuffer.getInt(it) }

    private fun leg(idx: Int) = legs[getTypeId(idx).toInt()]

    override fun isNull(idx: Int): Boolean {
        val typeId = getTypeId(idx)

        return (typeId == (-1).toByte()) || legs[typeId.toInt()].isNull(idx)
    }

    override fun writeNull() {
        typeBuffer.writeByte(-1)
        offsetBuffer.writeInt(0)
    }

    override fun getObject(idx: Int) = leg(idx).getObject(idx)

    override fun getObject0(idx: Int) = throw UnsupportedOperationException()
    override fun writeObject0(value: Any) = throw UnsupportedOperationException()

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        typeBuffer.unloadBuffer(buffers)
        offsetBuffer.unloadBuffer(buffers)

        legs.forEach { it.unloadBatch(nodes, buffers) }
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")

        typeBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing type buffer"))
        offsetBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing offset buffer"))
        legs.forEach { it.loadBatch(nodes, buffers) }

        valueCount = node.length
    }

    override fun reset() {
        typeBuffer.reset()
        offsetBuffer.reset()
        legs.forEach { it.reset() }
        valueCount = 0
    }

    override fun close() {
        typeBuffer.close()
        offsetBuffer.close()
        legs.forEach { it.close() }
        valueCount = 0
    }
}
