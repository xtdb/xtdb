package xtdb.arrow

import clojure.lang.ILookup
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Companion.slice
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.agg.VectorSummer
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.util.closeOnCatch
import java.nio.ByteBuffer

interface VectorReader : ILookup, AutoCloseable {
    val name: String
    val valueCount: Int

    val nullable: Boolean
    val arrowType: ArrowType
    val childFields: List<Field>
    val type get() = VectorType(arrowType, nullable, childFields)
    val fieldType: FieldType get() = type.fieldType
    val field get() = name ofType type

    private class RenamedVector(private val inner: VectorReader, override val name: String) : VectorReader by inner {
        override fun withName(newName: String) = RenamedVector(inner, newName)

        override fun select(idxs: IntArray) = inner.select(idxs).closeOnCatch { RenamedVector(it, name) }

        override fun select(startIdx: Int, len: Int) =
            inner.select(startIdx, len).closeOnCatch { RenamedVector(it, name) }

        override fun openSlice(al: BufferAllocator) =
            inner.openSlice(al).closeOnCatch { RenamedVector(it, name) }

        override fun openDirectSlice(al: BufferAllocator): Vector =
            inner.openDirectSlice(al).closeOnCatch { it.name = name; it }
    }

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

    fun valueReader(): ValueReader = ValueReader.ForVector(this)

    fun openSlice(al: BufferAllocator): VectorReader

    fun openDirectSlice(al: BufferAllocator): Vector =
        field.openVector(al).closeOnCatch { outVec -> outVec.also { it.append(this) } }

    fun select(idxs: IntArray): VectorReader = IndirectVector(this, selection(idxs))
    fun select(startIdx: Int, len: Int): VectorReader = IndirectVector(this, slice(startIdx, len))

    val asList get() = List(valueCount) { getObject(it) }
    fun toList(keyFn: IKeyFn<*>) = List(valueCount) { getObject(it, keyFn) }

    val metadataFlavours: Collection<MetadataFlavour> get() = unsupported("metadataFlavours")

    fun rowCopier(dest: VectorWriter): RowCopier

    companion object {
        fun toString(reader: VectorReader): String = reader.run {
            val content = when {
                valueCount == 0 -> ""
                valueCount <= 5 -> asList.joinToString(", ", prefix = " [", postfix = "]")
                else -> listOf(
                    getObject(0).toString(), getObject(1).toString(), getObject(2).toString(),
                    "...",
                    getObject(valueCount - 2).toString(), getObject(valueCount - 1).toString()
                ).joinToString(", ", prefix = " [", postfix = "]")
            }

            return "(${this::class.simpleName}[$valueCount]$content)"
        }
    }

    override fun valAt(key: Any?): Any? = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = this.vectorForOrNull(key as String) ?: notFound

    fun sumInto(outVec: Vector): VectorSummer = unsupported("sumInto")
}
