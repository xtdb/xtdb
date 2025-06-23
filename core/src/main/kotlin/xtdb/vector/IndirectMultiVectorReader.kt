package xtdb.vector

import clojure.lang.IFn
import clojure.lang.RT
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorIndirection
import xtdb.arrow.VectorPosition
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.arrow.unsupported
import xtdb.toLeg
import xtdb.util.Hasher
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

class IndirectMultiVectorReader(
    override val name: String,
    private val readers: List<VectorReader?>,
    private val readerIndirection: VectorIndirection,
    private val vectorIndirections: VectorIndirection,
) : IVectorReader {

    private val fields: List<Field?> = readers.map { it?.field }
    private val legReaders = ConcurrentHashMap<String, VectorReader>()
    private val vectorField by lazy(LazyThreadSafetyMode.PUBLICATION) {
        MERGE_FIELDS.applyTo(RT.seq(fields.filterNotNull())) as Field
    }

    companion object {
        private val MERGE_FIELDS: IFn = requiringResolve("xtdb.types/merge-fields")
    }

    init {
        assert(readers.any { it != null })
    }

    private fun unsupported(): RuntimeException {
        throw UnsupportedOperationException("IndirectMultiVectoReader")
    }

    private fun safeReader(idx: Int): VectorReader {
        return readers[readerIndirection[idx]] ?: throw unsupported()
    }

    override val valueCount: Int
        get() = readerIndirection.valueCount()

    override val field get() = vectorField

    override fun hashCode(idx: Int, hasher: Hasher): Int =
        safeReader(idx).hashCode(vectorIndirections[idx], hasher)

    override fun isNull(idx: Int): Boolean {
        val readerIdx = readerIndirection[idx]
        return readerIdx < 0 || readers[readerIdx] == null || readers[readerIdx]!!.isNull(vectorIndirections[idx])
    }

    override fun getBoolean(idx: Int): Boolean = safeReader(idx).getBoolean(vectorIndirections[idx])

    override fun getByte(idx: Int): Byte = safeReader(idx).getByte(vectorIndirections[idx])

    override fun getShort(idx: Int): Short = safeReader(idx).getShort(vectorIndirections[idx])

    override fun getInt(idx: Int): Int = safeReader(idx).getInt(vectorIndirections[idx])

    override fun getLong(idx: Int): Long = safeReader(idx).getLong(vectorIndirections[idx])

    override fun getFloat(idx: Int): Float = safeReader(idx).getFloat(vectorIndirections[idx])

    override fun getDouble(idx: Int): Double = safeReader(idx).getDouble(vectorIndirections[idx])

    override fun getBytes(idx: Int): ByteBuffer = safeReader(idx).getBytes(vectorIndirections[idx])

    override fun getPointer(idx: Int, reuse: ArrowBufPointer): ArrowBufPointer =
        safeReader(idx).getPointer(vectorIndirections[idx], reuse)

    override fun getObject(idx: Int): Any? = safeReader(idx).getObject(vectorIndirections[idx])

    override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any? =
        safeReader(idx).getObject(vectorIndirections[idx], keyFn)

    override val keyNames get() = readers.filterNotNull().flatMap { it.keyNames.orEmpty() }.toSet()

    // TODO - the following is a fairly dumb implementation requiring order O(n) where n is the total number of
    //        elements in all lists. Can we do something better?
    private fun range(start: Int, len: Int): IntArray = IntArray(len) { it + start }
    private fun repeat(len: Int, item: Int): IntArray = IntArray(len) { item }

    override val listElements: VectorReader
        get() {
            val listElementReaders = readers.map { it?.listElements }
            var readerIndirectionArray = intArrayOf()
            var vectorIndirectionArray = intArrayOf()
            for (i in 0 until this.valueCount) {
                if (readerIndirection[i] < 0 || readers[readerIndirection[i]] == null) continue
                val rdr = safeReader(i)
                val listCount = rdr.getListCount(vectorIndirections[i])
                readerIndirectionArray += repeat(listCount, readerIndirection[i])
                vectorIndirectionArray += range(rdr.getListStartIndex(vectorIndirections[i]), listCount)
            }
            return IndirectMultiVectorReader(
                "\$data\$",
                listElementReaders,
                VectorIndirection.selection(readerIndirectionArray),
                VectorIndirection.selection(vectorIndirectionArray)
            )
        }

    override fun getListStartIndex(idx: Int): Int =
        (0 until idx).sumOf { safeReader(it).getListCount(vectorIndirections[it]) }

    override fun getListCount(idx: Int): Int = safeReader(idx).getListCount(vectorIndirections[idx])

    override val mapKeys: VectorReader
        get() = IndirectMultiVectorReader("key", readers.map { it?.mapKeys }, readerIndirection, vectorIndirections)

    override val mapValues: VectorReader
        get() = IndirectMultiVectorReader("value", readers.map { it?.mapValues }, readerIndirection, vectorIndirections)

    override fun getLeg(idx: Int): String? {
        val reader = safeReader(idx)
        return when (val type = fields[readerIndirection[idx]]!!.fieldType.type) {
            is ArrowType.Union -> reader.getLeg(vectorIndirections[idx])
            else -> type.toLeg()
        }
    }

    override fun vectorForOrNull(name: String): VectorReader {
        return legReaders.computeIfAbsent(name) {
            val validReaders = readers.zip(fields).map { (reader, field) ->
                if (reader == null) null
                else when (field!!.fieldType.type) {
                    is ArrowType.Union -> reader.vectorFor(name)
                    else -> {
                        if (field.fieldType.type.toLeg() == name) reader
                        else null
                    }
                }
            }

            IndirectMultiVectorReader(
                name,
                validReaders,
                object : VectorIndirection {
                    override fun valueCount(): Int {
                        return readerIndirection.valueCount()
                    }

                    override fun getIndex(idx: Int): Int {
                        val readerIdx = readerIndirection[idx]
                        if (validReaders[readerIdx] != null) return readerIdx
                        return -1
                    }
                }, vectorIndirections
            )
        }
    }

    override val legNames: Set<String>
        get() =
            fields.flatMapIndexed { index: Int, field: Field? ->
                if (field != null) {
                    when (val type = field.fieldType.type) {
                        is ArrowType.Union -> readers[index]!!.legNames.orEmpty()
                        else -> listOf(type.toLeg())
                    }
                } else {
                    emptyList()
                }
            }.toSet()

    override fun copyTo(vector: ValueVector): VectorReader {
        val writer = writerFor(vector)
        val copier = rowCopier(writer)

        for (i in 0 until valueCount) {
            copier.copyRow(i)
        }

        writer.syncValueCount()
        return ValueVectorReader.from(vector)
    }

    override fun rowCopier(dest: VectorWriter): RowCopier {
        if (dest !is IVectorWriter) unsupported("IndirectMultiVectorReader.rowCopier(VectorWriter)")

        readers.map { it?.also { dest.promoteChildren(it.field) } }
        val rowCopiers =
            readers.map { it?.rowCopier(dest) ?: ValueVectorReader(NullVector(it?.name)).rowCopier(dest) }
        return RowCopier { sourceIdx -> rowCopiers[readerIndirection[sourceIdx]].copyRow(vectorIndirections[sourceIdx]) }
    }

    private fun indirectVectorPosition(pos: VectorPosition): VectorPosition {
        return object : VectorPosition {
            override var position: Int
                get() = vectorIndirections[pos.position]
                set(@Suppress("UNUSED_PARAMETER") value) {
                    throw unsupported()
                }
        }
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val indirectPos = indirectVectorPosition(pos)
        val valueReaders = readers.map { it?.valueReader(indirectPos) }.toTypedArray()

        return object : ValueReader {
            private fun valueReader(): ValueReader {
                return valueReaders[readerIndirection[pos.position]]!!
            }

            override val leg: String?
                get() = valueReader().leg

            override val isNull: Boolean
                get() = valueReader().isNull

            override fun readBoolean(): Boolean {
                return valueReader().readBoolean()
            }

            override fun readByte(): Byte {
                return valueReader().readByte()
            }

            override fun readShort(): Short {
                return valueReader().readShort()
            }

            override fun readInt(): Int {
                return valueReader().readInt()
            }

            override fun readLong(): Long {
                return valueReader().readLong()
            }

            override fun readFloat(): Float {
                return valueReader().readFloat()
            }

            override fun readDouble(): Double {
                return valueReader().readDouble()
            }

            override fun readBytes(): ByteBuffer {
                return valueReader().readBytes()
            }

            override fun readObject(): Any? {
                return valueReader().readObject()
            }
        }
    }

    override fun close() {
        readers.map { it?.close() }
    }

    override fun toString() = "(IndirectMultiVectorReader ${readers.map { it.toString() }})"
}
