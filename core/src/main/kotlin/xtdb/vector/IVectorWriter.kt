package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.toLeg
import java.nio.ByteBuffer

interface IVectorWriter : IValueWriter, AutoCloseable {
    /**
     *  Maintains the next position to be written to.
     *
     *  Automatically incremented by the various `write` methods, and any [IVectorWriter.rowCopier]s.
     */
    fun writerPosition(): IVectorPosition

    val vector: FieldVector

    val field: Field

    /**
     * This method calls [ValueVector.setValueCount] on the underlying vector, so that all of the values written
     * become visible through the Arrow Java API - we don't call this after every write because (for composite vectors, and especially unions)
     * it's not the cheapest call.
     */
    fun syncValueCount() {
        vector.valueCount = writerPosition().position
    }

    fun rowCopier(src: ValueVector): IRowCopier

    fun rowCopier(src: RelationReader): IRowCopier {
        val wp = writerPosition()
        val copiers = src.map { it.rowCopier(structKeyWriter(it.name)) }

        return IRowCopier { srcIdx ->
            val pos = wp.position
            startStruct()
            copiers.forEach { it.copyRow(srcIdx) }
            endStruct()
            pos
        }
    }

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
    override fun writeObject(obj: Any?): Unit = when {
        obj != null -> writeObject0(obj)
        !field.isNullable -> throw InvalidWriteObjectException(field, null)
        else -> writeNull()
    }
    fun writeObject0(obj: Any)

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

internal val UNION_FIELD_TYPE = FieldType.notNullable(ArrowType.Union(UnionMode.Dense, null))

internal data class InvalidWriteObjectException(val field: Field, val obj: Any?) :
    IllegalArgumentException("invalid writeObject")

internal data class InvalidCopySourceException(val src: Field, val dest: Field) :
    IllegalArgumentException("illegal copy src vector")

internal fun IVectorWriter.populateWithAbsents(pos: Int) =
    repeat(pos - writerPosition().position) { writeObject(null) }

internal data class FieldMismatch(val expected: FieldType, val given: FieldType) :
    IllegalArgumentException("Field type mismatch")

internal fun IVectorWriter.checkFieldType(given: FieldType) {
    val expected = field.fieldType
    if (expected.type != given.type || (given.isNullable && !expected.isNullable))
        throw FieldMismatch(expected, given)
}

internal fun IVectorWriter.promote(fieldType: FieldType, al: BufferAllocator): FieldVector {
    val field = this.field

    syncValueCount()

    return when {
        fieldType.type == ArrowType.Null.INSTANCE || (field.type == fieldType.type && fieldType.isNullable) ->
            vector
                .getTransferPair(Field(field.name, FieldType.nullable(field.type), field.children), al)
                .also { it.transfer() }
                .to as FieldVector

        field.type == ArrowType.Null.INSTANCE ->
            Field(field.name, FieldType.nullable(fieldType.type), emptyList())
                .createVector(al).also { it.valueCount = vector.valueCount }

        else -> {
            val duv = DenseUnionVector(field.name, al, UNION_FIELD_TYPE, null)
            val valueCount = vector.valueCount

            writerFor(duv).also { duvWriter ->
                val legWriter = duvWriter.legWriter(field.type.toLeg(), field.fieldType)
                vector.makeTransferPair(legWriter.vector).transfer()

                duvWriter.legWriter(fieldType.type.toLeg(), fieldType)
            }

            duv.apply {
                repeat(valueCount) { idx -> setTypeId(idx, 0); setOffset(idx, idx) }
                this.valueCount = valueCount
            }
        }
    }
}
