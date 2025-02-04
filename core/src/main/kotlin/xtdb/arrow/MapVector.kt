package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn

class MapVector(private val listVector: ListVector, private val keysSorted: Boolean) : Vector() {

    override var name
        get() = listVector.name
        set(value) {
            listVector.name = value
        }

    override var fieldType: FieldType
        get() = listVector.fieldType.copy(type = ArrowType.Map(keysSorted))
        set(value) {
            listVector.fieldType = value.copy(type = listVector.fieldType.type)
        }

    override val children get() = listVector.children

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
    // but Arrow Java does it this way, so for now we'll be consistent with them.
    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) = listVector.hashCode(idx, hasher)

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is MapVector)
        return listVector.rowCopier0(src.listVector)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any {
        val startIdx = listVector.getListStartIndex(idx)
        val entryCount = listVector.getListCount(idx)

        val elReader = listVector.elementReader()
        val keyReader = elReader.mapKeyReader()
        val valueReader = elReader.mapValueReader()

        return (0 until entryCount).associate { elIdx ->
            (startIdx + elIdx).let { entryIdx ->
                keyReader.getObject(entryIdx, keyFn) to valueReader.getObject(entryIdx, keyFn)
            }
        }
    }

    override fun writeObject0(value: Any) = when (value) {
        is Map<*, *> -> {
            val elWriter = listVector.elementWriter()
            val keyWriter = elWriter.mapKeyWriter()
            val valueWriter = elWriter.mapValueWriter()

            value.forEach { k, v ->
                keyWriter.writeObject(k)
                valueWriter.writeObject(v)
                elWriter.endStruct()
            }

            listVector.endList()
        }

        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun elementReader() = listVector.elementReader()
    override fun elementWriter() = listVector.elementWriter()
    override fun elementWriter(fieldType: FieldType) = listVector.elementWriter(fieldType)

    override fun endList() = listVector.endList()

    override fun mapKeyReader() = elementReader().mapKeyReader()
    override fun mapValueReader() = elementReader().mapValueReader()
    override fun mapKeyWriter() = elementWriter().mapKeyWriter()
    override fun mapKeyWriter(fieldType: FieldType) = elementWriter().mapKeyWriter(fieldType)
    override fun mapValueWriter() = elementWriter().mapValueWriter()
    override fun mapValueWriter(fieldType: FieldType) = elementWriter().mapValueWriter(fieldType)

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        listVector.unloadPage(nodes, buffers)

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) =
        listVector.loadPage(nodes, buffers)

    override fun loadFromArrow(vec: ValueVector) = listVector.loadFromArrow(vec)

    override fun clear() = listVector.clear()
    override fun close() = listVector.close()
}