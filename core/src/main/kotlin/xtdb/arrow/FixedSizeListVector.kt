package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import org.apache.arrow.vector.complex.FixedSizeListVector as ArrowFixedSizeListVector

class FixedSizeListVector(
    private val allocator: BufferAllocator,
    override var name: String,
    nullable: Boolean,
    private val listSize: Int,
    private var elVector: Vector
) : Vector() {

    override var fieldType: FieldType = FieldType(nullable, ArrowType.FixedSizeList(listSize), null)

    override val children get() = listOf(elVector)

    private val validityBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

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

        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun elementReader() = elVector

    override fun elementWriter() = elVector

    override fun elementWriter(fieldType: FieldType): VectorWriter =
        when {
            elVector.field.fieldType == fieldType -> elVector

            elVector is NullVector && elVector.valueCount == 0 ->
                fromField(allocator, Field("\$data\$", fieldType, emptyList())).also { elVector = it }

            else -> TODO("promote elVector")
        }

    override fun getListCount(idx: Int) = listSize
    override fun getListStartIndex(idx: Int) = idx * listSize

    override fun endList() {
        writeNotNull()
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) =
        (0 until listSize).fold(0) { hash, elIdx ->
            ByteFunctionHelpers.combineHash(hash, elVector.hashCode(idx * listSize + elIdx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is FixedSizeListVector)
        require(src.listSize == listSize)

        val elCopier = src.elVector.rowCopier(elVector)
        return RowCopier { srcIdx ->
            val startIdx = src.getListStartIndex(srcIdx)

            (startIdx until startIdx + listSize).forEach { elIdx ->
                elCopier.copyRow(elIdx)
            }

            valueCount.also { endList() }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)
        elVector.unloadPage(nodes, buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: throw IllegalStateException("missing node")

        validityBuffer.loadBuffer(buffers.removeFirst() ?: throw IllegalStateException("missing validity buffer"))
        elVector.loadPage(nodes, buffers)

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowFixedSizeListVector)

        validityBuffer.loadBuffer(vec.validityBuffer)
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
}