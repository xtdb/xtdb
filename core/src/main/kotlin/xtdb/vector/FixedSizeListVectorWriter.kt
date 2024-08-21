package xtdb.vector

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.ListValueReader
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorPosition

internal class FixedSizeListVectorWriter(
    override val vector: FixedSizeListVector,
    private val notify: FieldChangeListener?
) : IVectorWriter {
    private val listSize = (vector.field.type as FixedSizeList).listSize
    private val wp = VectorPosition.build(vector.valueCount)
    override var field: Field = vector.field

    private fun upsertElField(elField: Field) {
        field = Field(field.name, field.fieldType, listOf(elField))
        notify(field)
    }

    private var elWriter = writerFor(vector.dataVector, ::upsertElField)

    override fun writerPosition() = wp

    override fun clear() {
        super.clear()
        elWriter.clear()
    }

    override fun writeNull() {
        super.writeNull()
        repeat(listSize) { elWriter.writeNull() }
    }

    override fun listElementWriter(): IVectorWriter =
        if (vector.dataVector is NullVector) listElementWriter(UNION_FIELD_TYPE) else elWriter

    override fun listElementWriter(fieldType: FieldType): IVectorWriter {
        val res = vector.addOrGetVector<FieldVector>(fieldType)
        if (!res.isCreated) return elWriter

        val newDataVec = res.vector
        upsertElField(newDataVec.field)
        elWriter = writerFor(newDataVec, ::upsertElField).also { it.writerPosition().position = wp.position * listSize }
        return elWriter
    }

    override fun startList() {
        vector.startNewValue(wp.position)
    }

    override fun endList() {
        wp.getPositionAndIncrement()
    }

    private inline fun writeList(f: () -> Unit) {
        startList(); f(); endList()
    }

    override fun writeObject0(obj: Any) {
        writeList {
            when (obj) {
                is ListValueReader ->
                    repeat(obj.size()) { i ->
                        try {
                            elWriter.writeValue(obj.nth(i))
                        } catch (e: InvalidWriteObjectException) {
                            TODO("FSLV promotion")
                        }
                    }

                is List<*> -> obj.forEach {
                    try {
                        elWriter.writeObject(it)
                    } catch (e: InvalidWriteObjectException) {
                        TODO("FSLV promotion")
                    }
                }

                else -> throw InvalidWriteObjectException(field, obj)
            }
        }
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun promoteChildren(field: Field) {
        if (field.type != this.field.type || (field.isNullable && !this.field.isNullable))
            throw FieldMismatch(this.field.fieldType, field.fieldType)
        val child = field.children.single()
        if (
            (child.type != elWriter.field.type
                    || (child.isNullable && !elWriter.field.isNullable))
            && elWriter.field.type !is ArrowType.Union
        ) {
            TODO("FSLV promotion")
        }
        if (child.children.isNotEmpty()) elWriter.promoteChildren(child)
    }

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is FixedSizeListVector -> {
            if (src.field.isNullable && !field.isNullable
                || src.field.fieldType != field.fieldType)
                throw InvalidCopySourceException(src.field, field)

            val innerCopier = listElementWriter(src.dataVector.field.fieldType).rowCopier(src.dataVector)

            RowCopier { srcIdx ->
                wp.position.also {
                    if (src.isNull(srcIdx)) writeNull()
                    else writeList {
                        for (i in src.getElementStartIndex(srcIdx)..<src.getElementEndIndex(srcIdx)) {
                            innerCopier.copyRow(i)
                        }
                    }
                }
            }
        }

        else -> throw InvalidCopySourceException(src.field, field)
    }
}
