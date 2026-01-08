package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.util.closeOnCatch
import org.apache.arrow.vector.complex.FixedSizeListVector as ArrowFixedSizeListVector

class FixedSizeListVector private constructor(
    private val al: BufferAllocator,
    override var name: String, private val listSize: Int,
    private var validityBuffer: BitBuffer?,
    private var elVector: Vector,
    override var valueCount: Int = 0
) : Vector(), MetadataFlavour.List {

    override var nullable: Boolean
        get() = validityBuffer != null
        set(value) {
            if (value && validityBuffer == null)
                BitBuffer(al).also { validityBuffer = it }.writeOnes(valueCount)
        }

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean, listSize: Int, elVector: Vector, valueCount: Int = 0
    ) : this(al, name, listSize, if (nullable) BitBuffer(al) else null, elVector, valueCount)

    override val arrowType = ArrowType.FixedSizeList(listSize)

    override val vectors get() = listOf(elVector)

    override fun isNull(idx: Int) = nullable && !validityBuffer!!.getBoolean(idx)

    override fun writeUndefined() {
        validityBuffer?.writeBit(valueCount, 0)
        valueCount++
        repeat(listSize) { elVector.writeUndefined() }
    }

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    private fun writeNotNull() {
        validityBuffer?.writeBit(valueCount, 1)
        valueCount++
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): List<*> {
        return (idx * listSize until (idx + 1) * listSize).map { elVector.getObject(it, keyFn) }
    }

    override fun writeObject0(value: Any) = when (value) {
        is List<*> -> {
            require(value.size == listSize) { "invalid list size: expected $listSize, got ${value.size}" }
            value.forEach { elVector.writeObject(it) }
            writeNotNull()
        }

        is ListValueReader -> {
            val valueSize = value.size()
            require(valueSize == listSize) { "invalid list size: expected $listSize, got $valueSize" }
            repeat(value.size()) { elVector.writeValue(value.nth(it)) }
            writeNotNull()
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

            else -> TODO("promote elVector")
        }

    override fun getListCount(idx: Int) = listSize
    override fun getListStartIndex(idx: Int) = idx * listSize

    override fun endList() = writeNotNull()

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
            elVector = elVector.maybePromote(al, src.elVector.arrowType, src.elVector.nullable)
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

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), if (nullable) -1 else 0))
        if (nullable) validityBuffer?.unloadBuffer(buffers) else buffers.add(al.empty)
        elVector.unloadPage(nodes, buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst()
        valueCount = node.length

        val validityBuf = buffers.removeFirst()
        validityBuffer?.loadBuffer(validityBuf, valueCount)
        elVector.loadPage(nodes, buffers)
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowFixedSizeListVector)

        validityBuffer?.loadBuffer(vec.validityBuffer, vec.valueCount)
        elVector.loadFromArrow(vec.dataVector)

        valueCount = vec.valueCount
    }

    override fun clear() {
        validityBuffer?.clear()
        elVector.clear()
        valueCount = 0
    }

    override fun close() {
        validityBuffer?.close()
        elVector.close()
    }

    override fun openSlice(al: BufferAllocator) =
        validityBuffer?.openSlice(al).closeOnCatch { validityBuffer ->
            elVector.openSlice(al).closeOnCatch { elVector ->
                FixedSizeListVector(al, name, listSize, validityBuffer, elVector, valueCount)
            }
        }
}