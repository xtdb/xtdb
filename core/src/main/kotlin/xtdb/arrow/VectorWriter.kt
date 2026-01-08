package xtdb.arrow

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.PersistentArrayMap
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.kw
import java.nio.ByteBuffer

internal data class InvalidWriteObjectException(
    val vec: VectorWriter, val obj: Any?
) : IllegalStateException("invalid writeObject"), IExceptionInfo {
    override fun getData(): IPersistentMap =
        PersistentArrayMap.create(mapOf("target".kw to vec::class.simpleName, "obj".kw to obj))
}

internal data class InvalidCopySourceException(
    val srcType: ArrowType, val srcIsNullable: Boolean,
    val destType: ArrowType, val destIsNullable: Boolean
) :
    IllegalStateException(buildString {
        append("illegal copy src vector: ")
        append("$srcType ${if (srcIsNullable) "nullable" else "not null"}")
        append(" -> ")
        append("$destType ${if (destIsNullable) "nullable" else "not null"}")
    })

interface VectorWriter : VectorReader, AutoCloseable {

    override val nullable: Boolean

    fun writeUndefined()
    fun writeNull()

    fun writeBoolean(v: Boolean): Unit = unsupported("writeBoolean")
    fun writeByte(v: Byte): Unit = unsupported("writeByte")
    fun writeShort(v: Short): Unit = unsupported("writeShort")
    fun writeInt(v: Int): Unit = unsupported("writeInt")
    fun writeLong(v: Long): Unit = unsupported("writeLong")
    fun writeFloat(v: Float): Unit = unsupported("writeFloat")
    fun writeDouble(v: Double): Unit = unsupported("writeDouble")
    fun writeBytes(v: ByteBuffer): Unit = unsupported("writeBytes")
    fun writeBytes(v: ByteArray) = writeBytes(ByteBuffer.wrap(v))
    fun writeObject(obj: Any?)

    fun writeValue(v: ValueReader) = if (v.isNull) writeNull() else writeValue0(v)
    fun writeValue0(v: ValueReader)

    override fun vectorForOrNull(name: String): VectorWriter? = unsupported("vectorFor")

    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun get(name: String) = vectorFor(name)

    /**
     * @return a vector with the given name, creating/promoting if necessary
     */
    fun vectorFor(name: String, arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("vectorFor")

    fun endStruct(): Unit = unsupported("endStruct")

    override val listElements: VectorWriter get() = unsupported("listElements")
    fun getListElements(arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("getListElements")
    fun endList(): Unit = unsupported("endList")

    override val mapKeys: VectorWriter get() = unsupported("mapKeys")
    fun getMapKeys(arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("mapKeys")
    override val mapValues: VectorWriter get() = unsupported("mapValues")
    fun getMapValues(arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("getMapValues")

    fun clear()

    fun writeAll(vals: Iterable<Any?>) = apply { vals.forEach { writeObject(it) } }

    fun append(r: VectorReader) = appendRange(r, 0, r.valueCount)

    fun appendRows(r: VectorReader, sel: IntArray) = r.rowCopier(this).copyRows(sel)

    fun appendRange(r: VectorReader, startIdx: Int, length: Int) =
        r.rowCopier(this).copyRange(startIdx, length)
}
