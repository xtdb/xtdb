package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import java.nio.ByteBuffer

interface IVectorWriter : IValueWriter, AutoCloseable {
    /**
     *  Maintains the next position to be written to.
     *
     *  Automatically incremented by the various `write` methods, and any [IVectorWriter.rowCopier]s.
     */
    fun writerPosition(): IVectorPosition

    val vector: FieldVector

    val field: Field get() = vector.field

    /**
     * This method calls [ValueVector.setValueCount] on the underlying vector, so that all of the values written
     * become visible through the Arrow Java API - we don't call this after every write because (for composite vectors, and especially unions)
     * it's not the cheapest call.
     */
    fun syncValueCount() {
        vector.valueCount = writerPosition().position
    }

    fun rowCopier(src: ValueVector): IRowCopier

    private fun unsupported(method: String): Nothing =
        throw UnsupportedOperationException("$method not implemented for ${vector.javaClass.simpleName}")

    override fun writeNull() {
        vector.setNull(writerPosition().getPositionAndIncrement())
    }

    override fun writeBoolean(v: Boolean): Unit = unsupported("writeBoolean")
    override fun writeByte(v: Byte): Unit = unsupported("writeByte")
    override fun writeShort(v: Short): Unit = unsupported("writeShort")
    override fun writeInt(v: Int): Unit = unsupported("writeInt")
    override fun writeLong(v: Long): Unit = unsupported("writeLong")
    override fun writeFloat(v: Float): Unit = unsupported("writeFloat")
    override fun writeDouble(v: Double): Unit = unsupported("writeDouble")
    override fun writeBytes(v: ByteBuffer): Unit = unsupported("writeBytes")
    override fun writeObject(obj: Any?): Unit = unsupported("writeObject")

    fun writeValue(v: IValueReader) = if (v.isNull) writeNull() else writeValue0(v)
    fun writeValue0(v: IValueReader)

    fun structKeyWriter(key: String): IVectorWriter = unsupported("structKeyWriter")
    fun structKeyWriter(key: String, fieldType: FieldType): IVectorWriter = unsupported("structKeyWriter")
    fun startStruct(): Unit = unsupported("startStruct")
    fun endStruct(): Unit = unsupported("endStruct")

    fun listElementWriter(): IVectorWriter = unsupported("listElementWriter")
    fun listElementWriter(fieldType: FieldType): IVectorWriter = unsupported("listElementWriter")
    fun startList(): Unit = unsupported("startList")
    fun endList(): Unit = unsupported("endList")

    fun legWriter(leg: ArrowType): IVectorWriter = unsupported("legWriter")
    override fun legWriter(leg: Keyword): IVectorWriter = unsupported("legWriter")
    fun legWriter(leg: Keyword, fieldType: FieldType): IVectorWriter = unsupported("legWriter")

    fun clear() {
        vector.clear()
        writerPosition().position = 0
    }

    override fun close() {
        vector.close()
        writerPosition().position = 0
    }
}
