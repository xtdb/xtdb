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
import xtdb.api.query.IKeyFn
import xtdb.arrow.*
import xtdb.toLeg
import xtdb.util.Hasher
import xtdb.vector.extensions.SetType
import xtdb.vector.extensions.SetVector

interface IVectorWriter : VectorWriter, AutoCloseable {
    val vector: FieldVector

    override var valueCount: Int

    override val name: String get() = vector.name
    override val nullable get() = this.field.isNullable
    override val fieldType: FieldType get() = this.field.fieldType
    override val field: Field

    /**
     * This method calls [ValueVector.setValueCount] on the underlying vector, so that all the values written
     * become visible through the Arrow Java API.
     * we don't call this after every write because (for composite vectors, and especially unions)
     * it's not the cheapest call.
     */
    fun syncValueCount() {
        vector.valueCount = this.valueCount
    }

    override val asReader: VectorReader
        get() {
            syncValueCount()
            return ValueVectorReader.from(vector)
        }

    override fun openDirectSlice(al: BufferAllocator) = asReader.openDirectSlice(al)

    // This is essentially the promoteChildren for monomorphic vectors except NullVector
    fun promoteChildren(field: Field) {
        when {
            (field.type == ArrowType.Null.INSTANCE) -> {
                if (!this.field.isNullable)
                    throw FieldMismatch(this.field.fieldType, field.fieldType)
            }
            field.type is ArrowType.Union -> {
                val nullable = field.children.fold(false) { acc, field -> acc || field.type == ArrowType.Null.INSTANCE || field.isNullable }
                if (nullable && !this.field.isNullable)
                    throw FieldMismatch(this.field.fieldType, field.fieldType)

                val nonNullFields = field.children.filter { it.type != ArrowType.Null.INSTANCE }
                if ( nonNullFields.size > 1 || (nonNullFields.size == 1 && this.field.type != nonNullFields.first().type))
                    throw FieldMismatch(this.field.fieldType, field.fieldType)
            }
            else ->
                if (field.type != this.field.type || (field.isNullable && !this.field.isNullable))
                    throw FieldMismatch(this.field.fieldType, field.fieldType)
        }
    }

    override fun rowCopier(dest: VectorWriter) = unsupported("rowCopier(VectorWriter)")

    fun rowCopier(src: ValueVector): RowCopier

    override fun writeUndefined() = TODO("writeUndefined on IVectorWriter...? could probably do this")

    override fun writeNull() {
        vector.setNull(valueCount++)
    }

    override fun writeObject(obj: Any?): Unit = when {
        obj != null -> writeObject0(obj)
        !field.isNullable -> throw InvalidWriteObjectException(field.fieldType, null)
        else -> writeNull()
    }

    fun writeObject0(obj: Any)

    override fun endStruct(): Unit = unsupported("endStruct")

    override val listElements: IVectorWriter get() = unsupported("listElementWriter")
    override fun getListElements(fieldType: FieldType): IVectorWriter = unsupported("getListElements")
    override fun endList(): Unit = unsupported("endList")

    override fun vectorForOrNull(name: String): IVectorWriter? = unsupported("vectorForOrNull")
    override fun vectorFor(name: String): IVectorWriter = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun vectorFor(name: String, fieldType: FieldType): IVectorWriter = unsupported("vectorFor")

    override fun openSlice(al: BufferAllocator) = asReader.openSlice(al)

    // New VectorWriters are also VectorReaders; old ones weren't, so we throw unsupported
    override fun hashCode(idx: Int, hasher: Hasher) = unsupported("IVectorWriter/hashCode")
    override fun isNull(idx: Int): Boolean = unsupported("IVectorWriter/isNull")
    override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any? = unsupported("IVectorWriter/getObject")

    override fun clear() {
        vector.clear()
        valueCount = 0
    }

    override fun close() {
        vector.close()
        valueCount = 0
    }
}

internal val UNION_FIELD_TYPE = FieldType.notNullable(ArrowType.Union(UnionMode.Dense, null))

internal fun IVectorWriter.populateWithAbsents(pos: Int) =
    repeat(pos - valueCount) { writeObject(null) }

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
                        FieldType(valueCount > 0 || fieldType.isNullable, fieldType.type, null),
                        emptyList()
                    ).createVector(al).also { it.valueCount = vector.valueCount }
            }
        }

        else -> {
            val duv = DenseUnionVector(field.name, al, UNION_FIELD_TYPE, null)
            val valueCount = vector.valueCount

            writerFor(duv).also { duvWriter ->
                val legWriter = duvWriter.vectorFor(field.type.toLeg(), field.fieldType)
                vector.makeTransferPair(legWriter.vector).transfer()

                if (fieldType.type !is ArrowType.Union)
                    duvWriter.vectorFor(fieldType.type.toLeg(), fieldType)
            }

            duv.apply {
                repeat(valueCount) { idx -> setTypeId(idx, 0); setOffset(idx, idx) }
                this.valueCount = valueCount
            }
        }
    }
}
