package xtdb.vector

import clojure.lang.IFn
import clojure.lang.RT
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorIndirection
import xtdb.arrow.VectorPosition
import xtdb.toLeg
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

private val MERGE_FIELDS: IFn = requiringResolve("xtdb.types/merge-fields")

class IndirectMultiVectorReader(
    private val readers: List<IVectorReader?>,
    private val readerIndirection: VectorIndirection,
    private val vectorIndirections: VectorIndirection,
) : IVectorReader {

    private val name = readers.filterNotNull().first().name
    private val fields: List<Field?> = readers.map { it?.field }
    private val legReaders = ConcurrentHashMap<String, IVectorReader>()
    private val vectorField by lazy(LazyThreadSafetyMode.PUBLICATION) {
        MERGE_FIELDS.applyTo(RT.seq(fields.filterNotNull())) as Field
    }

    init {
        assert(readers.any { it != null })
    }

    private fun unsupported(): Nothing = throw UnsupportedOperationException("IndirectMultiVectorReader")

    private fun safeReader(idx: Int): IVectorReader = readers[readerIndirection[idx]] ?: unsupported()

    override fun valueCount(): Int = readerIndirection.valueCount()
    override fun getName(): String = name
    override fun getField(): Field = vectorField

    override fun hashCode(idx: Int, hasher: ArrowBufHasher) = safeReader(idx).hashCode(vectorIndirections[idx], hasher)

    override fun isNull(idx: Int): Boolean {
        val readerIdx = readerIndirection[idx]
        return readerIdx < 0 || readers[readerIdx] == null || readers[readerIdx]!!.isNull(vectorIndirections[idx])
    }

    override fun getBoolean(idx: Int) = safeReader(idx).getBoolean(vectorIndirections[idx])
    override fun getByte(idx: Int) = safeReader(idx).getByte(vectorIndirections[idx])
    override fun getShort(idx: Int) = safeReader(idx).getShort(vectorIndirections[idx])
    override fun getInt(idx: Int) = safeReader(idx).getInt(vectorIndirections[idx])
    override fun getLong(idx: Int) = safeReader(idx).getLong(vectorIndirections[idx])
    override fun getFloat(idx: Int) = safeReader(idx).getFloat(vectorIndirections[idx])
    override fun getDouble(idx: Int) = safeReader(idx).getDouble(vectorIndirections[idx])
    override fun getBytes(idx: Int): ByteBuffer = safeReader(idx).getBytes(vectorIndirections[idx])

    override fun getPointer(idx: Int): ArrowBufPointer = safeReader(idx).getPointer(vectorIndirections[idx])

    override fun getPointer(idx: Int, reuse: ArrowBufPointer): ArrowBufPointer =
        safeReader(idx).getPointer(vectorIndirections[idx], reuse)

    override fun getObject(idx: Int): Any? = safeReader(idx).getObject(vectorIndirections[idx])

    override fun getObject(idx: Int, keyFn: IKeyFn<*>?): Any? =
        safeReader(idx).getObject(vectorIndirections[idx], keyFn)

    override fun structKeyReader(colName: String) =
        IndirectMultiVectorReader(readers.map { it?.structKeyReader(colName) }, readerIndirection, vectorIndirections)

    override fun structKeys() = readers.filterNotNull().flatMap { it.structKeys() }.toSet()

    // TODO - the following is a fairly dumb implementation requiring order O(n) where n is the total number of
    //        elements in all lists. Can we do something better?
    private fun range(start: Int, len: Int): IntArray = IntArray(len) { it + start }
    private fun repeat(len: Int, item: Int): IntArray = IntArray(len) { item }

    override fun listElementReader(): IVectorReader {
        val listElementReaders = readers.map { it?.listElementReader() }
        var readerIndirectionArray = intArrayOf()
        var vectorIndirectionArray = intArrayOf()
        for (i in 0 until this.valueCount()) {
            if (readerIndirection[i] < 0 || readers[readerIndirection[i]] == null) continue
            val rdr = safeReader(i)
            val listCount = rdr.getListCount(vectorIndirections[i])
            readerIndirectionArray += repeat(listCount, readerIndirection[i])
            vectorIndirectionArray += range(rdr.getListStartIndex(vectorIndirections[i]), listCount)
        }
        return IndirectMultiVectorReader(
            listElementReaders,
            VectorIndirection.selection(readerIndirectionArray),
            VectorIndirection.selection(vectorIndirectionArray)
        )
    }

    override fun getListStartIndex(idx: Int): Int =
        (0 until idx).sumOf { safeReader(it).getListCount(vectorIndirections[it]) }

    override fun getListCount(idx: Int): Int = safeReader(idx).getListCount(vectorIndirections[idx])

    override fun mapKeyReader(): IVectorReader =
        IndirectMultiVectorReader(readers.map { it?.mapKeyReader() }, readerIndirection, vectorIndirections)

    override fun mapValueReader(): IVectorReader =
        IndirectMultiVectorReader(readers.map { it?.mapValueReader() }, readerIndirection, vectorIndirections)

    override fun getLeg(idx: Int): String =
        when (val type = fields[readerIndirection[idx]]!!.fieldType.type) {
            is ArrowType.Union -> safeReader(idx).getLeg(vectorIndirections[idx])
            else -> type.toLeg()
        }

    override fun legReader(legKey: String): IVectorReader =
        legReaders.computeIfAbsent(legKey) {
            val validReaders = readers.zip(fields).map { (reader, field) ->
                if (reader == null) null
                else when (field!!.fieldType.type) {
                    is ArrowType.Union -> reader.legReader(legKey)
                    else -> {
                        if (field.fieldType.type.toLeg() == legKey) reader
                        else null
                    }
                }
            }

            IndirectMultiVectorReader(
                validReaders,
                object : VectorIndirection {
                    override fun valueCount(): Int = readerIndirection.valueCount()

                    override fun getIndex(idx: Int): Int {
                        val readerIdx = readerIndirection[idx]
                        if (validReaders[readerIdx] != null) return readerIdx
                        return -1
                    }
                },
                vectorIndirections
            )
        }

    override fun legs(): List<String> =
        fields.flatMapIndexed { index: Int, field: Field? ->
            if (field != null) {
                when (val type = field.fieldType.type) {
                    is ArrowType.Union -> readers[index]!!.legs()
                    else -> listOf(type.toLeg())
                }
            } else emptyList()
        }.distinct()

    override fun copyTo(vector: ValueVector): IVectorReader {
        val writer = writerFor(vector)
        val copier = rowCopier(writer)

        repeat(valueCount()) { copier.copyRow(it) }

        writer.syncValueCount()
        return ValueVectorReader.from(vector)
    }

    override fun rowCopier(writer: IVectorWriter): RowCopier {

        readers.map { it?.also { writer.promoteChildren(it.field) } }
        val rowCopiers = readers.map { it?.rowCopier(writer) ?: ValueVectorReader(NullVector()).rowCopier(writer) }
        return RowCopier { sourceIdx -> rowCopiers[readerIndirection[sourceIdx]].copyRow(vectorIndirections[sourceIdx]) }
    }

    private fun indirectVectorPosition(pos: VectorPosition): VectorPosition {
        return object : VectorPosition {
            override var position: Int
                get() = vectorIndirections[pos.position]
                set(_) = unsupported()
        }
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val indirectPos = indirectVectorPosition(pos)
        val valueReaders = readers.map { it?.valueReader(indirectPos) }.toTypedArray()

        return object : ValueReader {
            private fun valueReader(): ValueReader = valueReaders[readerIndirection[pos.position]]!!

            override val leg: String? get() = valueReader().leg

            override val isNull: Boolean get() = valueReader().isNull
            override fun readBoolean(): Boolean = valueReader().readBoolean()
            override fun readByte(): Byte = valueReader().readByte()
            override fun readShort(): Short = valueReader().readShort()
            override fun readInt(): Int = valueReader().readInt()
            override fun readLong(): Long = valueReader().readLong()
            override fun readFloat(): Float = valueReader().readFloat()
            override fun readDouble(): Double = valueReader().readDouble()
            override fun readBytes(): ByteBuffer = valueReader().readBytes()
            override fun readObject(): Any? = valueReader().readObject()
        }
    }

    override fun close() {
        readers.forEach { it?.close() }
    }

    override fun toString() = "(IndirectMultiVectorReader ${readers.map { it.toString() }})"
}
