package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.UnionMode.Dense
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.toFieldType
import xtdb.toLeg
import xtdb.util.Hasher
import java.nio.ByteBuffer
import org.apache.arrow.vector.complex.DenseUnionVector as ArrowDenseUnionVector

internal val UNION_TYPE = ArrowType.Union(Dense, null)

class DenseUnionVector(
    private val allocator: BufferAllocator,
    override var name: String, legVectors: List<Vector>,
    override var valueCount: Int = 0
) : Vector() {

    override var nullable: Boolean
        get() = false
        set(_) { error("can't set DUV nullable") }

    override val type = UNION_TYPE

    companion object {
        internal fun promote(al: BufferAllocator, vector: Vector, target: FieldType) =
            if (vector is NullVector)
                fromField(al, Field(vector.name, target, emptyList()))
                    .also { newVec -> repeat(vector.valueCount) { newVec.writeNull() } }
            else
                DenseUnionVector(al, vector.name, listOf(vector), 0)
                    .apply {
                        vector.name = vector.fieldType.type.toLeg()

                        valueCount = vector.valueCount
                        repeat(vector.valueCount) { idx ->
                            typeBuffer.writeByte(0)
                            offsetBuffer.writeInt(idx)
                        }
                    }
    }

    private val legVectors = legVectors.toMutableList()

    override val children: Iterable<Vector> get() = legVectors

    inner class LegReader(private val typeId: Byte, private val inner: VectorReader) : VectorReader {
        override val name get() = inner.name
        override val nullable get() = inner.nullable
        override val valueCount get() = this@DenseUnionVector.valueCount
        override val fieldType get() = inner.fieldType
        override val field get() = inner.field

        override fun isNull(idx: Int) = getTypeId(idx) != typeId || inner.isNull(getOffset(idx))
        override fun getBoolean(idx: Int) = inner.getBoolean(getOffset(idx))
        override fun getByte(idx: Int) = inner.getByte(getOffset(idx))
        override fun getShort(idx: Int) = inner.getShort(getOffset(idx))
        override fun getInt(idx: Int) = inner.getInt(getOffset(idx))
        override fun getLong(idx: Int) = inner.getLong(getOffset(idx))
        override fun getFloat(idx: Int) = inner.getFloat(getOffset(idx))
        override fun getDouble(idx: Int) = inner.getDouble(getOffset(idx))
        override fun getBytes(idx: Int): ByteBuffer = inner.getBytes(getOffset(idx))
        override fun getObject(idx: Int, keyFn: IKeyFn<*>) = inner.getObject(getOffset(idx), keyFn)

        override fun getListCount(idx: Int) = inner.getListCount(getOffset(idx))
        override fun getListStartIndex(idx: Int) = inner.getListStartIndex(getOffset(idx))
        override val listElements get() = inner.listElements

        override val keyNames: Set<String>? get() = inner.keyNames
        override val legNames get() = inner.legNames
        override fun vectorForOrNull(name: String) = inner.vectorForOrNull(name)?.let { LegReader(typeId, it) }

        override val mapKeys get() = inner.mapKeys
        override val mapValues get() = inner.mapValues

        override fun hashCode(idx: Int, hasher: Hasher) = inner.hashCode(getOffset(idx), hasher)

        override fun rowCopier(dest: VectorWriter): RowCopier {
            val innerCopier = inner.rowCopier(dest)
            return RowCopier { srcIdx -> innerCopier.copyRow(getOffset(srcIdx)) }
        }

        override fun toList() = inner.toList()

        override fun close() = Unit
    }

    inner class LegWriter(private val typeId: Byte, val inner: Vector) :
        VectorReader by LegReader(typeId, inner), VectorWriter {

        private fun writeValueThen(): Vector {
            typeBuffer.writeByte(typeId)
            offsetBuffer.writeInt(inner.valueCount)
            this@DenseUnionVector.valueCount++
            return inner
        }

        override fun writeUndefined() = writeValueThen().writeUndefined()
        override fun writeNull() = writeValueThen().writeNull()

        override fun writeByte(v: Byte) = writeValueThen().writeByte(v)
        override fun writeShort(v: Short) = writeValueThen().writeShort(v)
        override fun writeInt(v: Int) = writeValueThen().writeInt(v)
        override fun writeLong(v: Long) = writeValueThen().writeLong(v)
        override fun writeFloat(v: Float) = writeValueThen().writeFloat(v)
        override fun writeDouble(v: Double) = writeValueThen().writeDouble(v)

        override fun writeBytes(v: ByteBuffer) = writeValueThen().writeBytes(v)
        override fun writeObject(obj: Any?) = writeValueThen().writeObject(obj)

        override fun vectorForOrNull(name: String) = inner.vectorForOrNull(name)
        override fun vectorFor(name: String) = inner.vectorFor(name)
        override fun vectorFor(name: String, fieldType: FieldType) = inner.vectorFor(name, fieldType)
        override fun endStruct() = writeValueThen().endStruct()

        override val listElements get() = inner.listElements
        override fun getListElements(fieldType: FieldType) = inner.getListElements(fieldType)
        override fun endList() = writeValueThen().endList()

        override val mapKeys get() = inner.mapKeys
        override fun getMapKeys(fieldType: FieldType) = inner.getMapKeys(fieldType)
        override val mapValues get() = inner.mapValues
        override fun getMapValues(fieldType: FieldType) = inner.getMapValues(fieldType)

        override fun rowCopier0(src: VectorReader): RowCopier {
            val innerCopier = src.rowCopier(inner)
            return RowCopier { srcIdx -> valueCount.also { writeValueThen(); innerCopier.copyRow(srcIdx) } }
        }

        override fun clear() = inner.clear()
        override fun close() = Unit

        override fun toList() = inner.toList()
    }

    private val typeBuffer = ExtensibleBuffer(allocator)
    private fun getTypeId(idx: Int) = typeBuffer.getByte(idx)
    internal fun typeIds() = (0 until valueCount).map { typeBuffer.getByte(it) }

    private val offsetBuffer = ExtensibleBuffer(allocator)
    private fun getOffset(idx: Int) = offsetBuffer.getInt(idx)
    internal fun offsets() = (0 until valueCount).map { offsetBuffer.getInt(it) }

    private fun leg(idx: Int) = getTypeId(idx).takeIf { it >= 0 }?.let { legVectors[it.toInt()] }

    override fun isNull(idx: Int) = leg(idx)?.isNull(getOffset(idx)) ?: true

    override fun writeUndefined() {
        typeBuffer.writeByte(-1)
        offsetBuffer.writeInt(0)
        valueCount++
    }

    override fun writeNull() {
        legWriter(FieldType.nullable(NULL_TYPE)).writeNull()
    }

    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = leg(idx)?.getObject(getOffset(idx), keyFn)
    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = throw UnsupportedOperationException()

    override fun writeObject0(value: Any) =
        value.toFieldType().let(::legWriter).writeObject(value)

    override fun getLeg(idx: Int) = leg(idx)?.name

    override val legNames get() = legVectors.mapTo(mutableSetOf()) { it.name }

    override fun vectorForOrNull(name: String): VectorWriter? {
        for (i in legVectors.indices) {
            val leg = legVectors[i]
            if (leg.name == name) return LegWriter(i.toByte(), leg)
        }

        return null
    }

    override fun vectorFor(name: String, fieldType: FieldType): VectorWriter {
        for (i in legVectors.indices) {
            val leg = legVectors[i]
            if (leg.name == name) {
                val legFieldType = leg.field.fieldType
                if (legFieldType.type != fieldType.type || (fieldType.isNullable && !legFieldType.isNullable))
                    TODO("promotion")

                return LegWriter(i.toByte(), leg)
            }
        }

        val typeId = legVectors.size.toByte()
        val legVec = fromField(allocator, Field(name, fieldType, emptyList())).also { legVectors.add(it) }
        return LegWriter(typeId, legVec)
    }

    private fun legWriter(fieldType: FieldType) = vectorFor(fieldType.type.toLeg(), fieldType)

    override fun valueReader(pos: VectorPosition): ValueReader {
        val legReaders = legVectors
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

    override fun hashCode0(idx: Int, hasher: Hasher) = leg(idx)!!.hashCode(getOffset(idx), hasher)

    override fun rowCopier0(src: VectorReader): RowCopier =
        when (src) {
            is DenseUnionVector -> {
                val copierMapping = src.legVectors.map { childVec ->
                    childVec.rowCopier(vectorFor(childVec.name, childVec.fieldType))
                }

                RowCopier { srcIdx ->
                    copierMapping[src.getTypeId(srcIdx).toInt().also { check(it >= 0) }].copyRow(src.getOffset(srcIdx))
                }
            }

            is NullVector -> {
                // we try to find a nullable child vector

                src.rowCopier(
                    legVectors.asSequence()
                        .mapIndexedNotNull { idx, vector ->
                            vector.takeIf { it.nullable }?.let { LegWriter(idx.toByte(), it) }
                        }
                        .firstOrNull()
                        ?: vectorFor("null", src.fieldType))
            }

            else -> {
                vectorFor(src.fieldType.type.toLeg()).rowCopier0(src)
            }
        }

    override fun rowCopier(dest: VectorWriter) =
        if (dest is DenseUnionVector)
            dest.rowCopier0(this).let { copier ->
                RowCopier { srcIdx ->
                    if (getTypeId(srcIdx) < 0) valueCount.also { dest.writeUndefined() } else copier.copyRow(srcIdx)
                }
            }
        else {
            require(legVectors.size == 1)
            LegWriter(0, legVectors.first()).rowCopier(dest)
        }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        typeBuffer.unloadBuffer(buffers)
        offsetBuffer.unloadBuffer(buffers)

        legVectors.forEach { it.unloadPage(nodes, buffers) }
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: throw IllegalStateException("missing node")

        typeBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing type buffer"))
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: throw IllegalStateException("missing offset buffer"))
        legVectors.forEach { it.loadPage(nodes, buffers) }

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowDenseUnionVector)
        val vc = vec.valueCount

        typeBuffer.loadBuffer(vec.typeBuffer, vc.toLong())
        offsetBuffer.loadBuffer(vec.offsetBuffer, vc.toLong() * Int.SIZE_BYTES)

        legVectors.forEach { it.loadFromArrow(vec.getChild(it.name)) }

        valueCount = vc
    }

    override fun openSlice(al: BufferAllocator) =
        DenseUnionVector(al, name, legVectors.map { it.openSlice(al) }, valueCount)

    override fun clear() {
        typeBuffer.clear()
        offsetBuffer.clear()
        legVectors.forEach { it.clear() }
        valueCount = 0
    }

    override fun close() {
        typeBuffer.close()
        offsetBuffer.close()
        legVectors.forEach { it.close() }
        valueCount = 0
    }
}
