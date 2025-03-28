package xtdb.arrow

import clojure.lang.ILookup
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Companion.slice
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.vector.IVectorReader
import xtdb.vector.IVectorWriter
import java.nio.ByteBuffer

interface VectorReader : ILookup, AutoCloseable {
    val name: String
    val valueCount: Int

    val nullable: Boolean
    val fieldType: FieldType
    val field: Field

    private class RenamedVector(private val inner: VectorReader, override val name: String) : VectorReader by inner

    fun withName(newName: String): VectorReader = RenamedVector(this, newName)

    fun isNull(idx: Int): Boolean
    fun getBoolean(idx: Int): Boolean = unsupported("getBoolean")
    fun getByte(idx: Int): Byte = unsupported("getByte")
    fun getShort(idx: Int): Short = unsupported("getShort")
    fun getInt(idx: Int): Int = unsupported("getInt")
    fun getLong(idx: Int): Long = unsupported("getLong")
    fun getFloat(idx: Int): Float = unsupported("getFloat")
    fun getDouble(idx: Int): Double = unsupported("getDouble")
    fun getBytes(idx: Int): ByteBuffer = unsupported("getBytes")
    fun getPointer(idx: Int, reuse: ArrowBufPointer = ArrowBufPointer()): ArrowBufPointer = unsupported("getPointer")

    fun getObject(idx: Int): Any? = getObject(idx) { it }
    fun getObject(idx: Int, keyFn: IKeyFn<*>): Any?

    fun hashCode(idx: Int, hasher: Hasher): Int

    val listElements: VectorReader get() = unsupported("listElements")
    fun getListStartIndex(idx: Int): Int = unsupported("getListStartIndex")
    fun getListCount(idx: Int): Int = unsupported("getListCount")

    val mapKeys: VectorReader get() = unsupported("mapKeys")
    val mapValues: VectorReader get() = unsupported("mapValueReader")

    val keyNames: Set<String>? get() = null
    val legNames: Set<String>? get() = null

    /**
     * @return an existing vector, or null if a vector doesn't exist with the given name
     */
    fun vectorForOrNull(name: String): VectorReader? = unsupported("vectorFor")

    /**
     * @return an existing vector
     * @throws IllegalStateException if the vector doesn't exist
     */
    fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")

    /**
     * convenience for `vectorFor(name)`
     */
    operator fun get(name: String) = vectorFor(name)

    /**
     * convenience for `getObject(idx)`
     */
    operator fun get(idx: Int) = getObject(idx)

    fun getLeg(idx: Int): String? = unsupported("getLeg")

    fun valueReader(pos: VectorPosition) = object : ValueReader {
        override val leg get() = getLeg(pos.position)

        override val isNull get() = isNull(pos.position)

        override fun readBoolean() = getBoolean(pos.position)
        override fun readByte() = getByte(pos.position)
        override fun readShort() = getShort(pos.position)
        override fun readInt() = getInt(pos.position)
        override fun readLong() = getLong(pos.position)
        override fun readFloat() = getFloat(pos.position)
        override fun readDouble() = getDouble(pos.position)
        override fun readBytes() = getBytes(pos.position)
        override fun readObject() = getObject(pos.position)
    }

    fun select(idxs: IntArray): VectorReader = IndirectVector(this, selection(idxs))
    fun select(startIdx: Int, len: Int): VectorReader = IndirectVector(this, slice(startIdx, len))

    fun toList() = List(valueCount) { getObject(it) }
    fun toList(keyFn: IKeyFn<*>) = List(valueCount) { getObject(it, keyFn) }

    val metadataFlavours: Collection<MetadataFlavour> get() = unsupported("metadataFlavours")

    fun rowCopier(dest: VectorWriter) =
        if (dest is DenseUnionVector) dest.rowCopier0(this)
        else {
            val copier = dest.rowCopier0(this)
            RowCopier { srcIdx ->
                if (isNull(srcIdx)) valueCount.also { dest.writeNull() } else copier.copyRow(srcIdx)
            }
        }

    companion object {
        fun toString(reader: VectorReader): String = reader.run {
            val content = when {
                valueCount == 0 -> ""
                valueCount <= 5 -> toList().joinToString(", ", prefix = " [", postfix = "]")
                else -> listOf(
                    getObject(0).toString(), getObject(1).toString(), getObject(2).toString(),
                    "...",
                    getObject(valueCount - 2).toString(), getObject(valueCount - 1).toString()
                ).joinToString(", ", prefix = " [", postfix = "]")
            }

            return "(${this::class.simpleName}[$valueCount]$content)"
        }

        internal class NewToOldAdapter(private val vector: VectorReader) : IVectorReader {

            override fun hashCode(idx: Int, hasher: Hasher) = vector.hashCode(idx, hasher)

            override val valueCount get() = vector.valueCount

            override val name get() = vector.name

            override val field get() = vector.field

            override fun isNull(idx: Int) = vector.isNull(idx)
            override fun getBoolean(idx: Int) = vector.getBoolean(idx)
            override fun getByte(idx: Int) = vector.getByte(idx)
            override fun getShort(idx: Int) = vector.getShort(idx)
            override fun getInt(idx: Int) = vector.getInt(idx)
            override fun getLong(idx: Int) = vector.getLong(idx)
            override fun getFloat(idx: Int) = vector.getFloat(idx)
            override fun getDouble(idx: Int) = vector.getDouble(idx)
            override fun getBytes(idx: Int) = vector.getBytes(idx)

            override fun getPointer(idx: Int, reuse: ArrowBufPointer) = vector.getPointer(idx, reuse)

            override fun getObject(idx: Int) = vector.getObject(idx)
            override fun getObject(idx: Int, keyFn: IKeyFn<*>) = getObject(idx)

            override fun structKeyReader(colName: String) = vector.vectorForOrNull(colName)?.let { NewToOldAdapter(it) }

            override val keyNames get() = TODO()

            override val listElements get() = NewToOldAdapter(vector.listElements)
            override fun getListStartIndex(idx: Int) = vector.getListStartIndex(idx)
            override fun getListCount(idx: Int) = vector.getListCount(idx)

            override val mapKeys get() = NewToOldAdapter(vector.mapKeys)
            override val mapValues get() = NewToOldAdapter(vector.mapValues)

            override fun getLeg(idx: Int) = vector.getLeg(idx)

            override fun legReader(legKey: String) = vector.vectorForOrNull(legKey)?.let { NewToOldAdapter(it) }

            override val legNames get() = vector.legNames

            override fun copyTo(vector: ValueVector) = error("copyTo")

            override fun rowCopier(writer: IVectorWriter) = error("rowCopier")

            override fun valueReader(pos: VectorPosition) = vector.valueReader(pos)

            override fun close() = vector.close()

            override fun toString(): String = "(NewToOldAdaptor{vector=$vector})"
        }

        private class OldToNewAdapter(private val old: IVectorReader) : VectorReader {
            override val name: String get() = old.name
            override val valueCount: Int get() = old.valueCount
            override val nullable: Boolean get() = this.field.isNullable
            override val fieldType: FieldType get() = this.field.fieldType
            override val field: Field get() = old.field

            override fun isNull(idx: Int) = old.isNull(idx)

            override fun getBoolean(idx: Int) = old.getBoolean(idx)
            override fun getByte(idx: Int) = old.getByte(idx)
            override fun getShort(idx: Int) = old.getShort(idx)
            override fun getInt(idx: Int) = old.getInt(idx)
            override fun getLong(idx: Int) = old.getLong(idx)
            override fun getFloat(idx: Int) = old.getFloat(idx)
            override fun getDouble(idx: Int) = old.getDouble(idx)
            override fun getBytes(idx: Int): ByteBuffer = old.getBytes(idx)
            override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any? = old.getObject(idx, keyFn)

            override fun getPointer(idx: Int, reuse: ArrowBufPointer): ArrowBufPointer = old.getPointer(idx, reuse)

            override fun hashCode(idx: Int, hasher: Hasher) = old.hashCode(idx, hasher)

            override val keyNames: Set<String>? get() = old.keyNames?.toSet()
            override fun vectorForOrNull(name: String) = old.structKeyReader(name)?.let { OldToNewAdapter(it) }
            override val listElements get() = OldToNewAdapter(old.listElements)

            override fun valueReader(pos: VectorPosition): ValueReader = old.valueReader(pos)

            override fun toList() = List(valueCount) { old.getObject(it) }

            override fun rowCopier(dest: VectorWriter) = error("rowCopier")

            override fun close() = old.close()

            override fun toString(): String = "(OldToNewAdaptor{oldReader=$old})"
        }

        @JvmStatic
        fun from(old: IVectorReader): VectorReader = OldToNewAdapter(old)

    }

    val asOldReader: IVectorReader get() = NewToOldAdapter(this)

    override fun valAt(key: Any?): Any? = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = this.vectorForOrNull(key as String) ?: notFound
}
