package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import org.apache.arrow.vector.complex.DenseUnionVector as ArrowDenseUnionVector

class DenseUnionVector(
    private val allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
    legs: List<Vector>
) : Vector() {

    private val legs = legs.toMutableList()

    inner class LegReader(private val typeId: Byte, private val inner: VectorReader) : VectorReader {
        override val name get() = inner.name
        override val nullable get() = inner.nullable
        override val valueCount get() = this@DenseUnionVector.valueCount
        override val field get() = inner.field

        override fun isNull(idx: Int) = getTypeId(idx) != typeId || inner.isNull(getOffset(idx))
        override fun getBoolean(idx: Int) = inner.getBoolean(getOffset(idx))
        override fun getByte(idx: Int) = inner.getByte(getOffset(idx))
        override fun getShort(idx: Int) = inner.getShort(getOffset(idx))
        override fun getInt(idx: Int) = inner.getInt(getOffset(idx))
        override fun getLong(idx: Int) = inner.getLong(getOffset(idx))
        override fun getFloat(idx: Int) = inner.getFloat(getOffset(idx))
        override fun getDouble(idx: Int) = inner.getDouble(getOffset(idx))
        override fun getBytes(idx: Int) = inner.getBytes(getOffset(idx))
        override fun getObject(idx: Int, keyFn: IKeyFn<*>) = inner.getObject(getOffset(idx), keyFn)

        override fun getListCount(idx: Int) = inner.getListCount(getOffset(idx))
        override fun getListStartIndex(idx: Int) = inner.getListStartIndex(getOffset(idx))
        override fun elementReader() = inner.elementReader()

        override fun keyReader(name: String) = inner.keyReader(name)?.let { LegReader(typeId, it) }
        override fun mapKeyReader() = inner.mapKeyReader()
        override fun mapValueReader() = inner.mapValueReader()

        override fun hashCode(idx: Int, hasher: ArrowBufHasher) = inner.hashCode(getOffset(idx), hasher)

        override fun rowCopier(dest: VectorWriter): RowCopier {
            val innerCopier = inner.rowCopier(dest)
            return RowCopier { srcIdx -> innerCopier.copyRow(getOffset(srcIdx)) }
        }

        override fun toList() = inner.toList()

        override fun close() = Unit
    }

    inner class LegWriter(
        private val typeId: Byte,
        val inner: Vector
    ) : VectorReader by LegReader(typeId, inner), VectorWriter {

        private fun writeValueThen(): Vector {
            typeBuffer.writeByte(typeId)
            offsetBuffer.writeInt(inner.valueCount)
            this@DenseUnionVector.valueCount++
            return inner
        }

        override fun writeNull() = writeValueThen().writeNull()

        override fun writeByte(value: Byte) = writeValueThen().writeByte(value)

        override fun writeShort(value: Short) = writeValueThen().writeShort(value)

        override fun writeInt(value: Int) = writeValueThen().writeInt(value)

        override fun writeLong(value: Long) = writeValueThen().writeLong(value)

        override fun writeFloat(value: Float) = writeValueThen().writeFloat(value)

        override fun writeDouble(value: Double) = writeValueThen().writeDouble(value)

        override fun writeBytes(bytes: ByteArray) = writeValueThen().writeBytes(bytes)

        override fun writeObject(value: Any?) = writeValueThen().writeObject(value)

        override fun keyWriter(name: String) = inner.keyWriter(name)
        override fun keyWriter(name: String, fieldType: FieldType) = inner.keyWriter(name, fieldType)
        override fun endStruct() = writeValueThen().endStruct()

        override fun elementWriter() = inner.elementWriter()
        override fun elementWriter(fieldType: FieldType) = inner.elementWriter(fieldType)
        override fun endList() = writeValueThen().endList()

        override fun rowCopier0(src: VectorReader): RowCopier {
            val innerCopier = inner.rowCopier0(src)
            return RowCopier { srcIdx -> valueCount.also { writeValueThen(); innerCopier.copyRow(srcIdx) } }
        }

        override fun reset() = inner.reset()
        override fun close() = Unit

        override fun toList() = inner.toList()
    }

    override val field = Field(name, FieldType.notNullable(MinorType.DENSEUNION.type), legs.map { it.field })

    private val typeBuffer = ExtensibleBuffer(allocator)
    private fun getTypeId(idx: Int) = typeBuffer.getByte(idx)
    internal fun typeIds() = (0 until valueCount).map { typeBuffer.getByte(it) }

    private val offsetBuffer = ExtensibleBuffer(allocator)
    private fun getOffset(idx: Int) = offsetBuffer.getInt(idx)
    internal fun offsets() = (0 until valueCount).map { offsetBuffer.getInt(it) }

    private fun leg(idx: Int) = getTypeId(idx).takeIf { it >= 0 }?.let { legs[it.toInt()] }

    override fun isNull(idx: Int) = leg(idx)?.isNull(getOffset(idx)) ?: true

    override fun writeNull() {
        typeBuffer.writeByte(-1)
        offsetBuffer.writeInt(0)
        valueCount++
    }

    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = leg(idx)?.getObject(getOffset(idx), keyFn)
    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = throw UnsupportedOperationException()
    override fun writeObject0(value: Any) = throw UnsupportedOperationException()

    override fun getLeg(idx: Int) = leg(idx)?.name

    override fun legReader(name: String): VectorReader? {
        for (i in legs.indices) {
            val leg = legs[i]
            if (leg.name == name) return LegReader(i.toByte(), leg)
        }

        return null
    }

    override fun legWriter(name: String): VectorWriter {
        for (i in legs.indices) {
            val leg = legs[i]
            if (leg.name == name) return LegWriter(i.toByte(), leg)
        }

        TODO("auto-creation")
    }

    override fun legWriter(name: String, fieldType: FieldType): VectorWriter {
        for (i in legs.indices) {
            val leg = legs[i]
            if (leg.name == name) {
                if (leg.field.fieldType != fieldType) TODO("promotion")

                return LegWriter(i.toByte(), leg)
            }
        }

        return LegWriter(
            legs.size.toByte(),
            fromField(allocator, Field(name, fieldType, emptyList())).also { legs.add(it) }
        )
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val legReaders = legs
            .mapIndexed { typeId, leg -> LegReader(typeId.toByte(), leg) }
            .associate { it.name to it.valueReader(pos) }

        return object : ValueReader {
            override val leg get() = this@DenseUnionVector.getLeg(pos.position)

            private val legReader get() = legReaders[leg]

            override val isNull: Boolean get() = legReader?.isNull ?: true
            override fun readBoolean() = legReader!!.readBoolean()
            override fun readByte() = legReader!!.readByte()
            override fun readShort() = legReader!!.readShort()
            override fun readInt() = legReader!!.readInt()
            override fun readLong() = legReader!!.readLong()
            override fun readFloat() = legReader!!.readFloat()
            override fun readDouble() = legReader!!.readDouble()
            override fun readBytes() = legReader!!.readBytes()
            override fun readObject() = legReader?.readObject()
        }
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = leg(idx)!!.hashCode(idx, hasher)

    override fun rowCopier0(src: VectorReader) =
        if (src is DenseUnionVector) {
            val copierMapping = src.legs.map { childVec ->
                val childField = childVec.field
                childVec.rowCopier(legWriter(childField.name, childField.fieldType))
            }

            RowCopier { srcIdx ->
                copierMapping[src.getTypeId(srcIdx).toInt().also { check(it >= 0) }].copyRow(src.getOffset(srcIdx))
            }
        } else {
            legWriter(src.name).rowCopier0(src)
        }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        typeBuffer.unloadBuffer(buffers)
        offsetBuffer.unloadBuffer(buffers)

        legs.forEach { it.unloadBatch(nodes, buffers) }
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: throw IllegalStateException("missing node")

        typeBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing type buffer"))
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing offset buffer"))
        legs.forEach { it.loadBatch(nodes, buffers) }

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowDenseUnionVector)
        typeBuffer.loadBuffer(vec.typeBuffer)
        offsetBuffer.loadBuffer(vec.offsetBuffer)

        legs.forEach { it.loadFromArrow(vec.getChild(it.name)) }

        valueCount = vec.valueCount
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
