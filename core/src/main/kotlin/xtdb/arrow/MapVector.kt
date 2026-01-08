package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher

class MapVector(private val listVector: ListVector, private val keysSorted: Boolean) : Vector() {

    override var name
        get() = listVector.name
        set(value) {
            listVector.name = value
        }

    override val arrowType = ArrowType.Map(keysSorted)

    override var nullable: Boolean
        get() = listVector.nullable
        set(value) {
            listVector.nullable = value
        }

    override val vectors get() = listVector.vectors

    override var valueCount: Int
        get() = listVector.valueCount
        set(value) {
            listVector.valueCount = value
        }

    override fun isNull(idx: Int) = listVector.isNull(idx)
    override fun writeUndefined() = listVector.writeUndefined()
    override fun writeNull() = listVector.writeNull()

    override fun getListCount(idx: Int) = listVector.getListCount(idx)
    override fun getListStartIndex(idx: Int) = listVector.getListStartIndex(idx)

    // this should technically be an unsorted hash, because maps are unordered
    // but Type Java does it this way, so for now we'll be consistent with them.
    // this is no longer true as we are doing our own hashing
    override fun hashCode0(idx: Int, hasher: Hasher) = listVector.hashCode(idx, hasher)

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is MapVector)
        return listVector.rowCopier0(src.listVector)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any {
        val startIdx = listVector.getListStartIndex(idx)
        val entryCount = listVector.getListCount(idx)

        val elReader = listVector.listElements
        val keyReader = elReader.mapKeys
        val valueReader = elReader.mapValues

        return (0 until entryCount).associate { elIdx ->
            (startIdx + elIdx).let { entryIdx ->
                keyReader.getObject(entryIdx, keyFn) to valueReader.getObject(entryIdx, keyFn)
            }
        }
    }

    override fun writeObject0(value: Any) = when (value) {
        is Map<*, *> -> {
            val elWriter = listVector.listElements
            val keyWriter = elWriter.mapKeys
            val valueWriter = elWriter.mapValues

            value.forEach { k, v ->
                keyWriter.writeObject(k)
                valueWriter.writeObject(v)
                elWriter.endStruct()
            }

            listVector.endList()
        }

        else -> throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val listElements get() = listVector.listElements
    override fun getListElements(arrowType: ArrowType, nullable: Boolean) = listVector.getListElements(arrowType, nullable)

    override fun endList() = listVector.endList()

    override val mapKeys get() = listElements.mapKeys
    override fun getMapKeys(arrowType: ArrowType, nullable: Boolean) = listElements.getMapKeys(arrowType, nullable)
    override val mapValues get() = listElements.mapValues
    override fun getMapValues(arrowType: ArrowType, nullable: Boolean) = listElements.getMapValues(arrowType, nullable)

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        listVector.unloadPage(nodes, buffers)

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        listVector.loadPage(nodes, buffers)

    override fun loadFromArrow(vec: ValueVector) = listVector.loadFromArrow(vec)

    override fun openSlice(al: BufferAllocator) = MapVector(listVector.openSlice(al), keysSorted)

    override fun clear() = listVector.clear()
    override fun close() = listVector.close()
}