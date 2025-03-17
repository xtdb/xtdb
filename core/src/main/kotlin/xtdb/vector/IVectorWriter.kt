package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.*
import xtdb.toLeg
import xtdb.vector.extensions.SetType
import xtdb.vector.extensions.SetVector
import java.nio.ByteBuffer

interface IVectorWriter : ValueWriter, AutoCloseable {
    /**
     *  Maintains the next position to be written to.
     *
     *  Automatically incremented by the various `write` methods, and any [IVectorWriter.rowCopier]s.
     */
    fun writerPosition(): VectorPosition

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

    fun promoteChildren(field: Field) {
        if (field.type == ArrowType.Null.INSTANCE) {
            if (!this.field.isNullable)
                throw FieldMismatch(this.field.fieldType, field.fieldType)
        } else {
            if (field.type != this.field.type || (field.isNullable && !this.field.isNullable))
                throw FieldMismatch(this.field.fieldType, field.fieldType)
        }
    }

    fun rowCopier(src: ValueVector): RowCopier

    fun rowCopier(src: RelationReader): RowCopier {
        val wp = writerPosition()
        val copiers = src.map { it.rowCopier(structKeyWriter(it.name)) }

        return RowCopier { srcIdx ->
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
        !field.isNullable -> throw InvalidWriteObjectException(field.fieldType, null)
        else -> writeNull()
    }

    fun writeObject0(obj: Any)

    fun writeValue(v: ValueReader) = if (v.isNull) writeNull() else writeValue0(v)
    fun writeValue0(v: ValueReader)

    fun structKeyWriter(key: String): IVectorWriter = unsupported("structKeyWriter")
    fun structKeyWriter(key: String, fieldType: FieldType): IVectorWriter = unsupported("structKeyWriter")
    fun startStruct(): Unit = unsupported("startStruct")
    fun endStruct(): Unit = unsupported("endStruct")

    fun listElementWriter(): IVectorWriter = unsupported("listElementWriter")
    fun listElementWriter(fieldType: FieldType): IVectorWriter = unsupported("listElementWriter")
    fun startList(): Unit = unsupported("startList")
    fun endList(): Unit = unsupported("endList")

    fun legWriter(leg: ArrowType): IVectorWriter = unsupported("legWriter")
    override fun legWriter(leg: String): IVectorWriter = unsupported("legWriter")
    fun legWriter(leg: String, fieldType: FieldType): IVectorWriter = unsupported("legWriter")

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

        // not union because unions don't have validity vectors - we need a specific null leg. #4153
        field.type == ArrowType.Null.INSTANCE && fieldType.type !is ArrowType.Union -> {
            when (fieldType.type) {
                // workaround for lists/sets for #3376
                ArrowType.List.INSTANCE ->
                    ListVector(field.name, al, FieldType.nullable(ArrowType.List.INSTANCE), null)
                        .also { it.valueCount = vector.valueCount }

                SetType -> SetVector(field.name, al, FieldType.nullable(SetType), null)
                    .also { it.valueCount = vector.valueCount }

                else ->
                    Field(
                        field.name,
                        FieldType(writerPosition().position > 0 || fieldType.isNullable, fieldType.type, null),
                        emptyList()
                    ).createVector(al).also { it.valueCount = vector.valueCount }
            }
        }

        else -> {
            val duv = DenseUnionVector(field.name, al, UNION_FIELD_TYPE, null)
            val valueCount = vector.valueCount

            writerFor(duv).also { duvWriter ->
                val legWriter = duvWriter.legWriter(field.type.toLeg(), field.fieldType)
                vector.makeTransferPair(legWriter.vector).transfer()

                if (fieldType.type !is ArrowType.Union)
                    duvWriter.legWriter(fieldType.type.toLeg(), fieldType)
            }

            duv.apply {
                repeat(valueCount) { idx -> setTypeId(idx, 0); setOffset(idx, idx) }
                this.valueCount = valueCount
            }
        }
    }
}

internal val IVectorWriter.asReader: IVectorReader get() {
    syncValueCount()
    return ValueVectorReader.from(vector)
}