package xtdb.vector

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.*

class ListVectorWriter(override val vector: ListVector, private val notify: FieldChangeListener?) : IVectorWriter {
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
        // see https://github.com/apache/arrow/issues/40796
        super.writeNull()
        vector.lastSet = valueCount - 1
    }

    override val listElements: IVectorWriter
        get() = if (vector.dataVector is NullVector) getListElements(UNION_FIELD_TYPE) else elWriter

    override fun getListElements(fieldType: FieldType): IVectorWriter {
        val res = vector.addOrGetVector<FieldVector>(fieldType)
        if (!res.isCreated) return elWriter

        val newDataVec = res.vector
        upsertElField(newDataVec.field)
        elWriter = writerFor(newDataVec, ::upsertElField)
        return elWriter
    }

    override fun endList() {
        val pos = valueCount++
        vector.startNewValue(pos)
        val endPos = elWriter.valueCount
        vector.endValue(pos, endPos - vector.getElementStartIndex(pos))
    }

    override fun writeObject0(obj: Any) {
        when (obj) {
            is ListValueReader ->
                for (i in 0..<obj.size())
                    elWriter.writeValue(obj.nth(i))

            is List<*> -> obj.forEach { elWriter.writeObject(it) }

            else -> throw InvalidWriteObjectException(field.fieldType, obj)
        }
        endList()
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is ListVector -> {
            if (src.field.isNullable && !field.isNullable)
                throw InvalidCopySourceException(src.field.fieldType, field.fieldType)

            val innerCopier = listElements.rowCopier(src.dataVector)

            RowCopier { srcIdx ->
                valueCount.also {
                    if (src.isNull(srcIdx)) writeNull()
                    else {
                        for (i in src.getElementStartIndex(srcIdx)..<src.getElementEndIndex(srcIdx)) {
                            innerCopier.copyRow(i)
                        }
                        endList()
                    }
                }
            }
        }

        else -> throw InvalidCopySourceException(src.field.fieldType, field.fieldType)
    }
}
