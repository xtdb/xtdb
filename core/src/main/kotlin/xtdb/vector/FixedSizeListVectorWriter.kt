package xtdb.vector

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.*

internal class FixedSizeListVectorWriter(
    override val vector: FixedSizeListVector,
    private val notify: FieldChangeListener?
) : IVectorWriter {
    private val listSize = (vector.field.type as FixedSizeList).listSize
    override var field: Field = vector.field

    private fun upsertElField(elField: Field) {
        field = Field(field.name, field.fieldType, listOf(elField))
        notify(field)
    }

    private var elWriter = writerFor(vector.dataVector, ::upsertElField)

    override var valueCount = vector.valueCount

    override fun clear() {
        super.clear()
        elWriter.clear()
    }

    override fun writeNull() {
        super.writeNull()
        repeat(listSize) { elWriter.writeUndefined() }
    }

    override val listElements: IVectorWriter
        get() = if (vector.dataVector is NullVector) getListElements(UNION_FIELD_TYPE) else elWriter

    override fun getListElements(fieldType: FieldType): IVectorWriter {
        val res = vector.addOrGetVector<FieldVector>(fieldType)
        if (!res.isCreated) return elWriter

        val newDataVec = res.vector
        upsertElField(newDataVec.field)
        elWriter = writerFor(newDataVec, ::upsertElField).also { it.valueCount = valueCount * listSize }
        return elWriter
    }

    override fun endList() {
        vector.startNewValue(valueCount++)
    }

    private inline fun writeList(f: () -> Unit) {
        f(); endList()
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

                else -> throw InvalidWriteObjectException(field.fieldType, obj)
            }
        }
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is FixedSizeListVector -> {
            if (src.field.isNullable && !field.isNullable
                || src.field.fieldType != field.fieldType)
                throw InvalidCopySourceException(src.field.fieldType, field.fieldType)

            val innerCopier = getListElements(src.dataVector.field.fieldType).rowCopier(src.dataVector)

            RowCopier { srcIdx ->
                valueCount.also {
                    if (src.isNull(srcIdx)) writeNull()
                    else writeList {
                        for (i in src.getElementStartIndex(srcIdx)..<src.getElementEndIndex(srcIdx)) {
                            innerCopier.copyRow(i)
                        }
                    }
                }
            }
        }

        else -> throw InvalidCopySourceException(src.field.fieldType, field.fieldType)
    }
}
