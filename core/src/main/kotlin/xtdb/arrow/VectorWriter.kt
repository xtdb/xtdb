package xtdb.arrow

import org.apache.arrow.vector.types.pojo.FieldType
import java.nio.ByteBuffer

interface VectorWriter : VectorReader, AutoCloseable {
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