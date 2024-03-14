package xtdb.vector

import clojure.lang.IFn
import clojure.lang.Keyword
import clojure.lang.RT
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.toLeg
import xtdb.util.requiringResolve
import xtdb.vector.ValueVectorReader.nullVector
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

class IndirectMultiVectorReader(
    private val readers: List<IVectorReader?>,
    private val readerIndirection: IVectorIndirection,
    private val vectorIndirections: IVectorIndirection,
) : IVectorReader {

    private val fields: List<Field?> = readers.map { it?.field }
    private val legReaders = ConcurrentHashMap<Keyword, IVectorReader>()

    companion object {
        private val MERGE_FIELDS: IFn = requiringResolve("xtdb.types/merge-fields")
        private val VEC_TO_WRITER: IFn = requiringResolve("xtdb.vector.writer/->writer")
    }

    init {
        assert(readers.any { it != null })
    }

    private fun unsupported(): RuntimeException {
        throw UnsupportedOperationException("IndirectMultiVectoReader")
    }

    private fun safeReader(idx: Int): IVectorReader {
        return readers[readerIndirection[idx]] ?: throw unsupported()
    }

    override fun valueCount(): Int {
        return readerIndirection.valueCount()
    }

    override fun getName(): String {
        return readers.filterNotNull().first().name
    }

    override fun withName(colName: String): IVectorReader {
        return RenamedVectorReader(this, colName)
    }

    override fun getField(): Field {
        return MERGE_FIELDS.applyTo(RT.seq(fields.filterNotNull())) as Field
    }

    override fun hashCode(idx: Int, hasher: ArrowBufHasher): Int {
        return safeReader(idx).hashCode(vectorIndirections[idx], hasher)
    }

    override fun isNull(idx: Int): Boolean {
        val readerIdx = readerIndirection[idx]
        return readerIdx < 0 || readers[readerIdx] == null || readers[readerIdx]!!.isNull(vectorIndirections[idx])
    }

    override fun isAbsent(idx: Int): Boolean {
        return safeReader(idx).isAbsent(vectorIndirections[idx])
    }

    override fun getBoolean(idx: Int): Boolean {
        return safeReader(idx).getBoolean(vectorIndirections[idx])
    }

    override fun getByte(idx: Int): Byte {
        return safeReader(idx).getByte(vectorIndirections[idx])
    }

    override fun getShort(idx: Int): Short {
        return safeReader(idx).getShort(vectorIndirections[idx])
    }

    override fun getInt(idx: Int): Int {
        return safeReader(idx).getInt(vectorIndirections[idx])
    }

    override fun getLong(idx: Int): Long {
        return safeReader(idx).getLong(vectorIndirections[idx])
    }

    override fun getFloat(idx: Int): Float {
        return safeReader(idx).getFloat(vectorIndirections[idx])
    }

    override fun getDouble(idx: Int): Double {
        return safeReader(idx).getDouble(vectorIndirections[idx])
    }

    override fun getBytes(idx: Int): ByteBuffer {
        return safeReader(idx).getBytes(vectorIndirections[idx])
    }

    override fun getPointer(idx: Int): ArrowBufPointer {
        return safeReader(idx).getPointer(vectorIndirections[idx])
    }

    override fun getPointer(idx: Int, reuse: ArrowBufPointer): ArrowBufPointer {
        return safeReader(idx).getPointer(vectorIndirections[idx], reuse)
    }

    override fun getObject(idx: Int): Any? {
        return safeReader(idx).getObject(vectorIndirections[idx])
    }

    override fun getObject(idx: Int, keyFn: IKeyFn<*>?): Any? {
        return safeReader(idx).getObject(vectorIndirections[idx], keyFn)
    }

    override fun structKeyReader(colName: String): IVectorReader {
        // TODO what happens if underlying readers have a different type?
        return IndirectMultiVectorReader(
            readers.map { it?.structKeyReader(colName) }, readerIndirection, vectorIndirections
        )
    }

    override fun structKeys(): Collection<String> {
        return readers.filterNotNull().flatMap { it.structKeys() }.toSet()
    }

    override fun listElementReader(): IVectorReader {
        // TODO what happens if underlying readers have a different type?
        return IndirectMultiVectorReader(
            readers.map { it?.listElementReader() }, readerIndirection, vectorIndirections
        )
    }

    override fun getListStartIndex(idx: Int): Int {
        return safeReader(idx).getListStartIndex(vectorIndirections[idx])
    }

    override fun getListCount(idx: Int): Int {
        return safeReader(idx).getListCount(vectorIndirections[idx])
    }

    override fun mapKeyReader(): IVectorReader {
        return IndirectMultiVectorReader(readers.map { it?.mapKeyReader() }, readerIndirection, vectorIndirections)
    }

    override fun mapValueReader(): IVectorReader {
        return IndirectMultiVectorReader(readers.map { it?.mapValueReader() }, readerIndirection, vectorIndirections)
    }

    override fun getLeg(idx: Int): Keyword {
        val reader = safeReader(idx)
        return when (val type = fields[readerIndirection[idx]]!!.fieldType.type) {
            is ArrowType.Union -> reader.getLeg(vectorIndirections[idx])
            else -> type.toLeg()
        }
    }

    private fun isUnion(field: Field): Boolean {
        return when (field.fieldType.type) {
            is ArrowType.Union -> true
            else -> false
        }
    }

    override fun legReader(legKey: Keyword): IVectorReader {
        return legReaders.computeIfAbsent(legKey) {
            val validReaders = readers.zip(fields).map { (reader, field) ->
                if (reader == null) null
                else when (val type = field!!.fieldType.type) {
                    is ArrowType.Union -> reader.legReader(legKey)
                    else -> {
                        if (field.fieldType.type.toLeg() == legKey) reader
                        else null
                    }
                }
            }

            IndirectMultiVectorReader(
                validReaders,
                object : IVectorIndirection {
                    override fun valueCount(): Int {
                        return readerIndirection.valueCount()
                    }

                    override fun getIndex(idx: Int): Int {
                        val readerIdx = readerIndirection[idx]
                        if (validReaders[readerIdx] != null) return readerIdx;
                        return -1
                    }
                }, vectorIndirections
            )
        }
    }

    override fun legs(): List<Keyword> {
        return fields.flatMapIndexed { index: Int, field: Field? ->
            if (field != null) {
                when (val type = field.fieldType.type) {
                    is ArrowType.Union -> readers[index]!!.legs()
                    else -> listOf(type.toLeg())
                }
            } else {
                emptyList()
            }
        }.toSet().toList()
    }

    override fun copyTo(vector: ValueVector): IVectorReader {
        val writer = VEC_TO_WRITER.invoke(vector) as IVectorWriter
        val copier = rowCopier(writer)

        for (i in 0 until valueCount()) {
            copier.copyRow(i)
        }

        writer.syncValueCount();
        return ValueVectorReader.from(vector);
    }

    override fun transferTo(vector: ValueVector): IVectorReader {
        throw unsupported()
    }

    override fun rowCopier(writer: IVectorWriter): IRowCopier {
        val rowCopiers = readers.map { it?.rowCopier(writer) ?: nullVector(NullVector()).rowCopier(writer) }
        return IRowCopier { sourceIdx -> rowCopiers[readerIndirection[sourceIdx]].copyRow(vectorIndirections[sourceIdx]) }
    }

    private fun indirectVectorPosition(pos: IVectorPosition): IVectorPosition {
        return object : IVectorPosition {
            override var position: Int
                get() = vectorIndirections[pos.position]
                set(value) {
                    throw unsupported()
                }
        }
    }

    override fun valueReader(pos: IVectorPosition): IValueReader {
        // simulating a duv in case the legs are non monomorphic
        if (legs().size > 1 || fields.filterNotNull().any(::isUnion)) {
            val legReaders = legs().associateWith { l -> legReader(l).valueReader(pos) }

            return object : IValueReader {
                override val leg: Keyword
                    get() = this@IndirectMultiVectorReader.getLeg(pos.position)

                private fun legReader(): IValueReader {
                    return legReaders[this.leg]!!
                }

                override val isNull: Boolean
                    get() = legReader().isNull

                override val isAbsent: Boolean
                    get() = legReader().isAbsent

                override fun readBoolean(): Boolean {
                    return legReader().readBoolean()
                }

                override fun readByte(): Byte {
                    return legReader().readByte()
                }

                override fun readShort(): Short {
                    return legReader().readShort()
                }

                override fun readInt(): Int {
                    return legReader().readInt()
                }

                override fun readLong(): Long {
                    return legReader().readLong()
                }

                override fun readFloat(): Float {
                    return legReader().readFloat()
                }

                override fun readDouble(): Double {
                    return legReader().readDouble()
                }

                override fun readBytes(): ByteBuffer {
                    return legReader().readBytes()
                }

                override fun readObject(): Any? {
                    return legReader().readObject()
                }
            }
        } else {
            val indirectPos = indirectVectorPosition(pos)
            val valueReaders = readers.map { it?.valueReader(indirectPos) }
            return object : IValueReader {

                private fun safeReader(): IValueReader {
                    return valueReaders[readerIndirection[pos.position]] ?: throw unsupported()
                }

                override val leg: Keyword?
                    get() = null
                override val isNull: Boolean
                    get() = safeReader().isNull
                override val isAbsent: Boolean
                    get() = safeReader().isAbsent

                override fun readBoolean(): Boolean {
                    return safeReader().readBoolean()
                }

                override fun readByte(): Byte {
                    return safeReader().readByte()
                }

                override fun readShort(): Short {
                    return safeReader().readShort()
                }

                override fun readInt(): Int {
                    return safeReader().readInt()
                }

                override fun readLong(): Long {
                    return safeReader().readLong()
                }

                override fun readFloat(): Float {
                    return safeReader().readFloat()
                }

                override fun readDouble(): Double {
                    return safeReader().readDouble()
                }

                override fun readBytes(): ByteBuffer {
                    return safeReader().readBytes()
                }

                override fun readObject(): Any? {
                    return safeReader().readObject()
                }
            }
        }
    }

    override fun close() {
        readers.map { it?.close() }
    }

    override fun toString(): String {
        return "(IndirectMultiVectorReader ".plus(readers.map { it.toString() }.toString()).plus(")")
    }
}