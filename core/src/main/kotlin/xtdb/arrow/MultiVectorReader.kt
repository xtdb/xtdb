package xtdb.arrow

import clojure.lang.IFn
import clojure.lang.RT
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.toLeg
import xtdb.util.Hasher
import xtdb.util.closeAllOnCatch
import xtdb.util.requiringResolve
import xtdb.util.safeMap
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

class MultiVectorReader(
    private val readers: List<VectorReader?>,
    private val readerIndirection: VectorIndirection,
    private val vectorIndirections: VectorIndirection,
) : VectorReader {

    override val name = readers.filterNotNull().first().name
    private val fields = readers.map { it?.field }
    private val legReaders = ConcurrentHashMap<String, VectorReader>()
    override val nullable get() = this.field.isNullable

    companion object {
        private val MERGE_FIELDS: IFn = requiringResolve("xtdb.types/merge-fields")
    }

    override val field by lazy(LazyThreadSafetyMode.PUBLICATION) {
        MERGE_FIELDS.applyTo(RT.seq(fields.filterNotNull())) as Field
    }

    override val fieldType: FieldType get() = this.field.fieldType

    init {
        assert(readers.any { it != null })
    }

    private fun reader(idx: Int) = readers[readerIndirection[idx]]!!

    override val valueCount get() = readerIndirection.valueCount()

    override fun hashCode(idx: Int, hasher: Hasher): Int {
        return reader(idx).hashCode(vectorIndirections[idx], hasher)
    }

    override fun isNull(idx: Int): Boolean {
        val readerIdx = readerIndirection[idx]
        return readerIdx < 0 || readers[readerIdx] == null || readers[readerIdx]!!.isNull(vectorIndirections[idx])
    }

    override fun getBoolean(idx: Int): Boolean = reader(idx).getBoolean(vectorIndirections[idx])

    override fun getByte(idx: Int): Byte = reader(idx).getByte(vectorIndirections[idx])

    override fun getShort(idx: Int): Short = reader(idx).getShort(vectorIndirections[idx])

    override fun getInt(idx: Int): Int = reader(idx).getInt(vectorIndirections[idx])

    override fun getLong(idx: Int): Long = reader(idx).getLong(vectorIndirections[idx])

    override fun getFloat(idx: Int): Float = reader(idx).getFloat(vectorIndirections[idx])

    override fun getDouble(idx: Int): Double = reader(idx).getDouble(vectorIndirections[idx])

    override fun getBytes(idx: Int): ByteBuffer = reader(idx).getBytes(vectorIndirections[idx])

    override fun getPointer(idx: Int, reuse: ArrowBufPointer): ArrowBufPointer =
        reader(idx).getPointer(vectorIndirections[idx], reuse)

    override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any? = reader(idx).getObject(vectorIndirections[idx], keyFn)

    override fun vectorForOrNull(name: String) =
        MultiVectorReader(readers.map { it?.vectorForOrNull(name) }, readerIndirection, vectorIndirections)

    override val listElements: VectorReader
        get() = MultiVectorReader(
            readers.map { it?.listElements }, readerIndirection, vectorIndirections
        )

    override fun getListStartIndex(idx: Int): Int = reader(idx).getListStartIndex(vectorIndirections[idx])

    override fun getListCount(idx: Int): Int = reader(idx).getListCount(vectorIndirections[idx])

    override val mapKeys: VectorReader
        get() = MultiVectorReader(readers.map { it?.mapKeys }, readerIndirection, vectorIndirections)

    override val mapValues: VectorReader
        get() = MultiVectorReader(readers.map { it?.mapValues }, readerIndirection, vectorIndirections)

    override fun getLeg(idx: Int): String? {
        val reader = reader(idx)
        return when (val type = fields[readerIndirection[idx]]!!.fieldType.type) {
            is ArrowType.Union -> reader.getLeg(vectorIndirections[idx])
            else -> type.toLeg()
        }
    }

    override fun vectorFor(name: String): VectorReader {
        return legReaders.computeIfAbsent(name) {
            val validReaders = readers.zip(fields).map { (reader, field) ->
                if (reader == null) null
                else when (field!!.fieldType.type) {
                    is ArrowType.Union -> reader.vectorForOrNull(name)
                    else -> {
                        if (field.fieldType.type.toLeg() == name) reader
                        else null
                    }
                }
            }

            MultiVectorReader(
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

    override fun rowCopier(dest: VectorWriter): RowCopier {
        // TODO promote
//        readers.map { it?.also { writer.promoteChildren(it.field) }}
        val rowCopiers = readers.map { it?.rowCopier(dest) ?: NullVector("null").rowCopier(dest) }
        return RowCopier { sourceIdx -> rowCopiers[readerIndirection[sourceIdx]].copyRow(vectorIndirections[sourceIdx]) }
    }

    private fun indirectVectorPosition(pos: VectorPosition) =
        object : VectorPosition {
            override var position: Int
                get() = vectorIndirections[pos.position]
                @Suppress("UNUSED_PARAMETER")
                set(value) = error("set indirectVectorPosition")
        }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val indirectPos = indirectVectorPosition(pos)
        val valueReaders = readers.map { it?.valueReader(indirectPos) }

        return object : ValueReader {
            private fun valueReader() = valueReaders[readerIndirection[pos.position]]!!

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

    override fun openSlice(al: BufferAllocator): VectorReader =
        readers
            .safeMap { it?.openSlice(al) }
            .closeAllOnCatch { MultiVectorReader(it, readerIndirection, vectorIndirections) }

    override fun toString() = VectorReader.toString(this)

    override fun close() = readers.forEach { it?.close() }
}