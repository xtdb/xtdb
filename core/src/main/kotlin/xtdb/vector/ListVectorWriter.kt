package xtdb.vector

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.replaceDataVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.isSubType
import xtdb.toFieldType
import xtdb.toLeg
import org.apache.arrow.vector.types.pojo.ArrowType.Union as UNION_TYPE

class ListVectorWriter(override val vector: ListVector, override val notify: FieldChangeListener?) : IVectorWriter {
    private val wp = IVectorPosition.build(vector.valueCount)
    override var field: Field = vector.field

    private fun upsertElField(elField: Field) {
        field = Field(field.name, field.fieldType, listOf(elField))
        notify(field)
    }

    private var elWriter = writerFor(vector.dataVector, ::upsertElField)

    private fun promoteElWriter(fieldType: FieldType): IVectorWriter {
        val newVec = elWriter.promote(fieldType, vector.allocator)
        vector.replaceDataVector(newVec)
        upsertElField(newVec.field)
        return writerFor(newVec, ::upsertElField).also { elWriter = it }
    }

    override fun writerPosition() = wp

    override fun clear() {
        super.clear()
        elWriter.clear()
    }

    override fun writeNull() {
        // see https://github.com/apache/arrow/issues/40796
        super.writeNull()
        vector.lastSet = wp.position - 1
    }

    override fun listElementWriter(): IVectorWriter = elWriter
        //if (vector.dataVector is NullVector) listElementWriter(UNION_FIELD_TYPE) else elWriter

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
        writeList {
            when (obj) {
                is IListValueReader ->
                    for (i in 0..<obj.size()) {
                        try {
                            elWriter.writeValue(obj.nth(i))
                        } catch (e: InvalidWriteObjectException) {
                            promoteElWriter(e.obj.toFieldType()).writeObject(obj.nth(i))
                        }
                    }

                is List<*> -> obj.forEach {
                    try {
                        elWriter.writeObject(it)
                    } catch (e: InvalidWriteObjectException) {
                        promoteElWriter(e.obj.toFieldType()).writeObject(it)
                    }
                }

                else -> throw InvalidWriteObjectException(field, obj)
            }
        }
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())

    override fun maybePromote(field: Field): IVectorWriter {
        return when {
            field.type is ArrowType.List && (!field.isNullable || this.field.isNullable) -> {
                if (field.children[0].fieldType != vector.dataVector.field.fieldType && vector.dataVector.field.type !is UNION_TYPE)
                    promoteElWriter(field.children[0].fieldType)
                elWriter = elWriter.maybePromote(field.children[0])
                upsertElField(elWriter.field)
                this
            }
            else -> {
                val newWriter = writerFor(promote(field.fieldType, vector.allocator)).also {
                    if (it is DenseUnionVectorWriter) it.legWriter(field.type.toLeg()).maybePromote(field)
                }
                notify(newWriter.field)
                newWriter.writerPosition().position = writerPosition().position
                newWriter
            }
        }
    }

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is ListVector -> {
            assert(isSubType(src.field, field))

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
