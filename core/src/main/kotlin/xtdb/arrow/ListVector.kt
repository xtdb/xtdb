package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Listy
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import org.apache.arrow.vector.complex.ListVector as ArrowListVector

internal val LIST: ArrowType.List = ArrowType.List.INSTANCE

class ListVector private constructor(
    private val al: BufferAllocator,
    override var name: String,
    private var elVector: Vector,
    private var validityBuffer: BitBuffer?,
    private val offsetBuffer: ExtensibleBuffer,
    override var valueCount: Int = 0
) : MonoVector(), MetadataFlavour.List {

    override var nullable: Boolean
        get() = validityBuffer != null
        set(value) {
            if (value && validityBuffer == null)
                BitBuffer(al).also { validityBuffer = it }.writeOnes(valueCount)
        }

    @JvmOverloads
    constructor(
        allocator: BufferAllocator, name: String, nullable: Boolean, elVector: Vector = NullVector("\$data$"),
    ) : this(
        allocator, name, elVector,
        if (nullable) BitBuffer(allocator) else null, ExtensibleBuffer(allocator)
    )

    override val arrowType: ArrowType = LIST
    override val monoType: Listy get() = Listy(LIST, elVector.type)

    override val vectors get() = listOf(elVector)

    private var lastOffset: Int = 0

    override fun isNull(idx: Int) = nullable && !validityBuffer!!.getBoolean(idx)

    private fun writeOffset(newOffset: Int) {
        if (valueCount == 0) offsetBuffer.writeInt(0)
        offsetBuffer.writeInt(newOffset)
        lastOffset = newOffset
    }

    override fun writeUndefined() {
        writeOffset(lastOffset)
        validityBuffer?.writeBit(valueCount, 0)
        valueCount++
    }

    private fun writeNotNull(len: Int) {
        writeOffset(lastOffset + len)
        validityBuffer?.writeBit(valueCount, 1)
        valueCount++
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
                    elVector = elVector.maybePromote(al, e.obj.toArrowType(), e.obj == null)
                    elVector.writeObject(el)
                }
            }
        }

        is ListValueReader -> {
            val len = value.size()
            writeNotNull(len)

            for (i in 0..<len) {
                val el = value.nth(i)
                try {
                    elVector.writeValue(el)
                } catch (e: InvalidWriteObjectException) {
                    elVector = elVector.maybePromote(al, e.obj.toArrowType(), e.obj == null)
                    elVector.writeValue(el)
                }
            }
        }

        else -> throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val listElements get() = elVector

    override fun getListElements(arrowType: ArrowType, nullable: Boolean): VectorWriter =
        when {
            elVector.arrowType == arrowType && elVector.nullable == nullable -> elVector

            elVector is NullVector && elVector.valueCount == 0 ->
                al.openVector("\$data\$", arrowType, nullable).also { elVector = it }

            else -> elVector.maybePromote(al, arrowType, nullable).also { elVector = it }
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
        check(src is ListVector)

        val elCopier = try {
            src.elVector.rowCopier(elVector)
        } catch (_: InvalidCopySourceException) {
            elVector = elVector.maybePromote(al, src.elVector.arrowType, src.elVector.nullable)
            src.elVector.rowCopier(elVector)
        }

        return RowCopier { srcIdx ->
            if (src.isNull(srcIdx)) {
                writeNull()
            } else {
                val startIdx = src.getListStartIndex(srcIdx)
                elCopier.copyRange(startIdx, src.getListEndIndex(srcIdx) - startIdx)

                endList()
            }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), if (nullable) -1 else 0))
        if (nullable) validityBuffer?.unloadBuffer(buffers) else buffers.add(al.empty)
        offsetBuffer.unloadBuffer(buffers)
        elVector.unloadPage(nodes, buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")
        valueCount = node.length

        val validityBuf = buffers.removeFirstOrNull() ?: error("missing validity buffer")
        validityBuffer?.loadBuffer(validityBuf, valueCount)
        offsetBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing offset buffer"))

        elVector.loadPage(nodes, buffers)
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowListVector)

        validityBuffer?.loadBuffer(vec.validityBuffer, vec.valueCount)
        offsetBuffer.loadBuffer(vec.offsetBuffer)
        elVector.loadFromArrow(vec.dataVector)

        valueCount = vec.valueCount
    }

    override fun openSlice(al: BufferAllocator) =
        ListVector(
            al, name, elVector.openSlice(al),
            validityBuffer?.openSlice(al),
            offsetBuffer.openSlice(al),
            valueCount
        )

    override fun valueReader() = object : ValueReader {
        override var pos = 0

        val elValueReader = elVector.valueReader()

        val listValReader = object : ListValueReader {
            override fun size(): Int = getListCount(pos)

            override fun nth(idx: Int): ValueReader {
                elValueReader.pos = getListStartIndex(pos) + idx
                return elValueReader
            }
        }

        override val isNull get() = this@ListVector.isNull(pos)
        override fun readObject() = listValReader
    }

    override fun clear() {
        validityBuffer?.clear()
        offsetBuffer.clear()
        elVector.clear()
        valueCount = 0
        lastOffset = 0
    }

    override fun close() {
        validityBuffer?.close()
        offsetBuffer.close()
        elVector.close()
        valueCount = 0
        lastOffset = 0
    }

}
