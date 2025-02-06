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

    override val nullable: Boolean

    fun writeUndefined()
    fun writeNull()

    fun writeBoolean(value: Boolean): Unit = unsupported("writeBoolean")
    fun writeByte(value: Byte): Unit = unsupported("writeByte")
    fun writeShort(value: Short): Unit = unsupported("writeShort")
    fun writeInt(value: Int): Unit = unsupported("writeInt")
    fun writeLong(value: Long): Unit = unsupported("writeLong")
    fun writeFloat(value: Float): Unit = unsupported("writeFloat")
    fun writeDouble(value: Double): Unit = unsupported("writeDouble")
    fun writeBytes(buf: ByteBuffer): Unit = unsupported("writeBytes")
    fun writeObject(value: Any?)

    fun keyWriter(name: String): VectorWriter = unsupported("keyWriter")
    fun keyWriter(name: String, fieldType: FieldType): VectorWriter = unsupported("keyWriter")
    fun endStruct(): Unit = unsupported("endStruct")

    fun elementWriter(): VectorWriter = unsupported("elementWriter")
    val elementWriter get() = elementWriter()

    fun elementWriter(fieldType: FieldType): VectorWriter = unsupported("elementWriter")
    fun endList(): Unit = unsupported("endList")

    fun legWriter(name: String): VectorWriter = unsupported("legWriter")
    fun legWriter(name: String, fieldType: FieldType): VectorWriter = unsupported("legWriter")

    fun mapKeyWriter(): VectorWriter = unsupported("mapKeyWriter")
    fun mapKeyWriter(fieldType: FieldType): VectorWriter = unsupported("mapKeyWriter")
    fun mapValueWriter(): VectorWriter = unsupported("mapValueWriter")
    fun mapValueWriter(fieldType: FieldType): VectorWriter = unsupported("mapValueWriter")

    fun rowCopier0(src: VectorReader): RowCopier

    fun clear()

    fun writeAll(vals: Iterable<Any?>) = apply { vals.forEach { writeObject(it) } }
}
