package xtdb.vector

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class ListVectorWriter(override val vector: ListVector, private val notify: FieldChangeListener?) : IVectorWriter {
    private val wp = IVectorPosition.build(vector.valueCount)
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

    override fun listElementWriter(): IVectorWriter =
        if (vector.dataVector is NullVector) listElementWriter(UNION_FIELD_TYPE) else elWriter

    override fun listElementWriter(fieldType: FieldType): IVectorWriter {
        val res = vector.addOrGetVector<FieldVector>(fieldType)
        if (!res.isCreated) return elWriter

        val newDataVec = res.vector
        upsertElField(newDataVec.field)
        elWriter = writerFor(newDataVec, ::upsertElField)
        return elWriter
    }

    override fun startList() {
        vector.startNewValue(wp.position)
    }

    override fun endList() {
        val pos = wp.getPositionAndIncrement()
        val endPos = elWriter.writerPosition().position
        vector.endValue(pos, endPos - vector.getElementStartIndex(pos))
    }

    private inline fun writeList(f: () -> Unit) {
        startList(); f(); endList()
    }

    override fun writeObject0(obj: Any) {
        val elWtr = listElementWriter()

        writeList {
            when (obj) {
                is IListValueReader -> {
                    for (i in 0..<obj.size()) {
                        elWtr.writeValue(obj.nth(i))
                    }
                }

                is Collection<*> -> obj.forEach { elWtr.writeObject(it) }

                else -> throw InvalidWriteObjectException(field, obj)
            }
        }
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())

    override fun legWriter(leg: ArrowType) = monoLegWriter(leg)

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is ListVector -> {
            val innerCopier = listElementWriter().rowCopier(src.dataVector)

            IRowCopier { srcIdx ->
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
