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
import xtdb.toFieldType
import xtdb.util.Hasher
import org.apache.arrow.vector.complex.ListVector as ArrowListVector

internal val LIST: ArrowType.List = ArrowType.List.INSTANCE

class ListVector private constructor(
    private val al: BufferAllocator,
    override var name: String, override var nullable: Boolean,
    private var elVector: Vector,
    private val validityBuffer: ExtensibleBuffer,
    private val offsetBuffer: ExtensibleBuffer,
    override var valueCount: Int = 0
) : Vector(), MetadataFlavour.List {

    @JvmOverloads
    constructor(
        allocator: BufferAllocator, name: String, nullable: Boolean, elVector: Vector = NullVector("\$data$"),
    ) : this(
        allocator, name, nullable, elVector,
        ExtensibleBuffer(allocator), ExtensibleBuffer(allocator)
    )

    override val type: ArrowType = LIST

    override val vectors: Iterable<Vector> get() = listOf(elVector)


    private var lastOffset: Int = 0

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    private fun writeOffset(newOffset: Int) {
        if (valueCount == 0) offsetBuffer.writeInt(0)
        offsetBuffer.writeInt(newOffset)
        lastOffset = newOffset
    }

    override fun writeUndefined() {
        writeOffset(lastOffset)
        validityBuffer.writeBit(valueCount++, 0)
    }

    private fun writeNotNull(len: Int) {
        writeOffset(lastOffset + len)
        validityBuffer.writeBit(valueCount++, 1)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): List<*> {
        val start = offsetBuffer.getInt(idx)
        val end = offsetBuffer.getInt(idx + 1)

        return (start until end).map { elVector.getObject(it, keyFn) }
    }

    override fun writeObject0(value: Any) = when (value) {
        is List<*> -> {
            writeNotNull(value.size)

            value.forEach { el ->
                try {
                    elVector.writeObject(el)
                } catch (e: InvalidWriteObjectException) {
                    elVector = elVector.maybePromote(al, e.obj.toFieldType())
                    elVector.writeObject(el)
                }
            }
        }

        is ListValueReader ->
            for (i in 0..<value.size()) {
                val el = value.nth(i)
                try {
                    elVector.writeValue(el)
                } catch (e: InvalidWriteObjectException) {
                    elVector = DenseUnionVector.promote(al, elVector, e.obj.toFieldType())
                    elVector.writeValue(el)
                }
            }

        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val listElements get() = elVector

    override fun getListElements(fieldType: FieldType): VectorWriter =
        when {
            elVector.field.fieldType == fieldType -> elVector

            elVector is NullVector && elVector.valueCount == 0 ->
                fromField(al, Field("\$data\$", fieldType, emptyList())).also { elVector = it }

            else -> elVector.maybePromote(al, fieldType).also { elVector = it }
        }

    override fun endList() = writeNotNull(elVector.valueCount - lastOffset)

    override fun getListCount(idx: Int) = getListEndIndex(idx) - getListStartIndex(idx)
    override fun getListStartIndex(idx: Int) = offsetBuffer.getInt(idx)
    internal fun getListEndIndex(idx: Int) = offsetBuffer.getInt(idx + 1)

    override val metadataFlavours get() = listOf(this)

    override fun hashCode0(idx: Int, hasher: Hasher) =
        (getListStartIndex(idx) until getListEndIndex(idx)).fold(0) { hash, elIdx ->
            ByteFunctionHelpers.combineHash(hash, elVector.hashCode(elIdx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is ListVector)
        val elCopier = src.elVector.rowCopier(elVector)
        return RowCopier { srcIdx ->
            (src.getListStartIndex(srcIdx) until src.getListEndIndex(srcIdx)).forEach { elIdx ->
                elCopier.copyRow(elIdx)
            }

            valueCount.also { endList() }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        offsetBuffer.unloadBuffer(buffers)
        elVector.unloadPage(nodes, buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")

        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing validity buffer"))
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing offset buffer"))

        elVector.loadPage(nodes, buffers)

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowListVector)

        validityBuffer.loadBuffer(vec.validityBuffer)
        offsetBuffer.loadBuffer(vec.offsetBuffer)
        elVector.loadFromArrow(vec.dataVector)

        valueCount = vec.valueCount
    }

    override fun openSlice(al: BufferAllocator) =
        ListVector(
            al, name, nullable, elVector.openSlice(al),
            validityBuffer.openSlice(al),
            offsetBuffer.openSlice(al),
            valueCount
        )

    override fun valueReader(pos: VectorPosition): ValueReader {
        val elPos = VectorPosition.build()
        val elValueReader = elVector.valueReader(elPos)

        val listValReader = object : ListValueReader {
            override fun size(): Int = getListCount(pos.position)

            override fun nth(idx: Int): ValueReader {
                elPos.position = getListStartIndex(pos.position) + idx
                return elValueReader
            }
        }

        return object : ValueReader {
            override val isNull get() = this@ListVector.isNull(pos.position)
            override fun readObject() = listValReader
        }
    }

    override fun clear() {
        validityBuffer.clear()
        offsetBuffer.clear()
        elVector.clear()
        valueCount = 0
        lastOffset = 0
    }

    override fun close() {
        validityBuffer.close()
        offsetBuffer.close()
        elVector.close()
        valueCount = 0
        lastOffset = 0
    }

}
