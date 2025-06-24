package xtdb.arrow

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.PersistentArrayMap
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.asKeyword
import java.nio.ByteBuffer

internal data class InvalidWriteObjectException(val fieldType: FieldType, val obj: Any?) :
    IllegalArgumentException("invalid writeObject"), IExceptionInfo {
    override fun getData(): IPersistentMap =
        PersistentArrayMap.create(mapOf("field-type".asKeyword to fieldType, "obj".asKeyword to obj))
}

internal data class InvalidCopySourceException(val src: FieldType, val dest: FieldType) :
    IllegalArgumentException("illegal copy src vector")

interface VectorWriter : VectorReader, AutoCloseable {

    /**
     * to ease xtdb.arrow migration - once xtdb.vector is gone we can inline this
     */
    val asReader: VectorReader get() = this

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
    fun writeObject(obj: Any?)

    fun writeValue(v: ValueReader) = if (v.isNull) writeNull() else writeValue0(v)
    fun writeValue0(v: ValueReader)

    override fun vectorForOrNull(name: String): VectorWriter? = unsupported("vectorFor")

    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun get(name: String) = vectorFor(name)

    /**
     * @return a vector with the given name, creating/promoting if necessary
     */
    fun vectorFor(name: String, fieldType: FieldType): VectorWriter = unsupported("vectorFor")

    fun endStruct(): Unit = unsupported("endStruct")

    override val listElements: VectorWriter get() = unsupported("listElements")
    fun getListElements(fieldType: FieldType): VectorWriter = unsupported("getListElements")
    fun endList(): Unit = unsupported("endList")

    override val mapKeys: VectorWriter get() = unsupported("mapKeys")
    fun getMapKeys(fieldType: FieldType): VectorWriter = unsupported("mapKeys")
    override val mapValues: VectorWriter get() = unsupported("mapValues")
    fun getMapValues(fieldType: FieldType): VectorWriter = unsupported("getMapValues")

    fun clear()

    fun writeAll(vals: Iterable<Any?>) = apply { vals.forEach { writeObject(it) } }

    fun append(r: VectorReader) {
        val copier = r.rowCopier(this)
        repeat(r.valueCount) { idx -> copier.copyRow(idx) }
    }
}
