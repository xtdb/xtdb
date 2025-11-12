package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.util.closeOnCatch
import org.apache.arrow.vector.complex.FixedSizeListVector as ArrowFixedSizeListVector

class FixedSizeListVector private constructor(
    private val al: BufferAllocator,
    override var name: String, override var nullable: Boolean, private val listSize: Int,
    private val validityBuffer: BitBuffer = BitBuffer(al),
    private var elVector: Vector,
    override var valueCount: Int = 0
) : Vector(), MetadataFlavour.List {

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean, listSize: Int, elVector: Vector, valueCount: Int = 0
    ) : this(al, name, nullable, listSize, BitBuffer(al), elVector, valueCount)

    override val arrowType = ArrowType.FixedSizeList(listSize)

    override val vectors get() = listOf(elVector)

    override fun isNull(idx: Int) = !validityBuffer.getBoolean(idx)

    override fun writeUndefined() {
        validityBuffer.writeBit(valueCount++, 0)
        repeat(listSize) { elVector.writeUndefined() }
    }

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    private fun writeNotNull() = validityBuffer.writeBit(valueCount++, 1)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): List<*> {
        return (idx * listSize until (idx + 1) * listSize).map { elVector.getObject(it, keyFn) }
    }

    override fun writeObject0(value: Any) = when (value) {
        is List<*> -> {
            require(value.size == listSize) { "invalid list size: expected $listSize, got ${value.size}" }
            writeNotNull()
            value.forEach { elVector.writeObject(it) }
        }

        is ListValueReader -> {
            val valueSize = value.size()
            require(valueSize == listSize) { "invalid list size: expected $listSize, got $valueSize" }
            writeNotNull()
            repeat(value.size()) { elVector.writeValue(value.nth(it)) }
        }

        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val listElements get() = elVector

    override fun getListElements(fieldType: FieldType): VectorWriter =
        when {
            elVector.field.fieldType == fieldType -> elVector

            elVector is NullVector && elVector.valueCount == 0 ->
                Field("\$data\$", fieldType, emptyList()).openVector(al).also { elVector = it }

            else -> elVector.maybePromote(al, fieldType).also { elVector = it }
        }

    fun maybePromoteElement(targetFieldType: FieldType) {
        if (elVector.field.fieldType != targetFieldType) {
            elVector = elVector.maybePromote(al, targetFieldType)
        }
    }

    override fun getListCount(idx: Int) = listSize
    override fun getListStartIndex(idx: Int) = idx * listSize

    override fun endList() {
        writeNotNull()
    }

    override fun valueReader(): ValueReader = object : ValueReader {
        override var pos = 0

        val elValueReader = elVector.valueReader()

        val listValReader = object : ListValueReader {
            override fun size(): Int = listSize

            override fun nth(idx: Int): ValueReader {
                elValueReader.pos = getListStartIndex(pos) + idx
                return elValueReader
            }
        }

        override val isNull get() = this@FixedSizeListVector.isNull(pos)
        override fun readObject() = listValReader
    }

    override val metadataFlavours get() = listOf(this)

    override fun hashCode0(idx: Int, hasher: Hasher) =
        (0 until listSize).fold(0) { hash, elIdx ->
            ByteFunctionHelpers.combineHash(hash, elVector.hashCode(idx * listSize + elIdx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        check(src is FixedSizeListVector)
        check(src.listSize == listSize)

        val elCopier = try {
            src.elVector.rowCopier(elVector)
        } catch (_: InvalidCopySourceException) {
            elVector = elVector.maybePromote(al, src.elVector.fieldType)
            src.elVector.rowCopier(elVector)
        }

        return RowCopier { srcIdx ->
            if (src.isNull(srcIdx)) writeNull()
            else {
                elCopier.copyRange(src.getListStartIndex(srcIdx), listSize)

                endList()
            }
        }
    }

    override fun openUnloadedPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.openUnloadedBuffer(buffers)
        elVector.openUnloadedPage(nodes, buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst()
        valueCount = node.length

        validityBuffer.loadBuffer(buffers.removeFirst(), valueCount)
        elVector.loadPage(nodes, buffers)
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowFixedSizeListVector)

        validityBuffer.loadBuffer(vec.validityBuffer, vec.valueCount)
        elVector.loadFromArrow(vec.dataVector)

        valueCount = vec.valueCount
    }

    override fun clear() {
        validityBuffer.clear()
        elVector.clear()
        valueCount = 0
    }

    override fun close() {
        validityBuffer.close()
        elVector.close()
    }

    override fun openSlice(al: BufferAllocator) =
        validityBuffer.openSlice(al).closeOnCatch { validityBuffer ->
            elVector.openSlice(al).closeOnCatch { elVector ->
                FixedSizeListVector(al, name, nullable, listSize, validityBuffer, elVector, valueCount)
            }
        }
}