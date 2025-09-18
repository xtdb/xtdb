package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.InvalidCopySourceException
import xtdb.arrow.InvalidWriteObjectException
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.error.Incorrect
import xtdb.toFieldType
import xtdb.util.normalForm
import org.apache.arrow.vector.types.pojo.ArrowType.Union as UNION_TYPE

class StructVectorWriter(override val vector: StructVector, private val notify: FieldChangeListener?) : IVectorWriter,
    Iterable<Map.Entry<String, IVectorWriter>> {
    override var field: Field = vector.field

    private val childFields: MutableMap<String, Field> =
        field.children.associateByTo(HashMap()) { childField -> childField.name }

    override var valueCount = vector.valueCount

    private fun upsertChildField(childField: Field) {
        childFields[childField.name] = childField
        field = Field(field.name, field.fieldType, childFields.values.toList())
        notify(field)
    }

    private fun writerFor(child: ValueVector) = writerFor(child, ::upsertChildField)

    private val writers: MutableMap<String, IVectorWriter> =
        vector.associateTo(HashMap()) { childVec -> childVec.name to writerFor(childVec) }

    override fun iterator() = writers.iterator()

    override fun clear() {
        super.clear()
        writers.forEach { (_, w) -> w.clear() }
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun writeNull() {
        super.writeNull()
        writers.values.forEach(IVectorWriter::writeNull)
    }

    private fun IVectorWriter.writeChildObject(v: Any?) {
        if (v is ValueReader) writeValue(v) else writeObject(v)
    }

    override fun writeObject0(obj: Any) {
        if (obj !is Map<*, *>) throw InvalidWriteObjectException(field.fieldType, obj)

        val structPos = valueCount
        for ((k, v) in obj) {
            val key = when (k) {
                is Keyword -> normalForm(k.sym.toString())
                is String -> k
                else -> throw IllegalArgumentException("invalid struct key: '$k'")
            }

            val writer = writers[key] ?: newChildWriter(key, v.toFieldType())

            if (writer.valueCount != structPos)
                throw Incorrect(errorCode = "xtdb/key-already-set", data = mapOf("ks" to obj.keys, "k" to k))

            writer.writeChildObject(v)
        }
        endStruct()
    }

    private fun newChildWriter(key: String, fieldType: FieldType): IVectorWriter {
        val pos = valueCount
        val fieldType1 = if (pos == 0) fieldType else FieldType(true, fieldType.type, fieldType.dictionary)

        return writerFor(vector.addOrGet(key, fieldType1, FieldVector::class.java))
            .also {
                upsertChildField(it.field)
                writers[key] = it
                it.populateWithAbsents(pos)
            }
    }

    override fun vectorForOrNull(name: String) = writers[name]

    override fun vectorFor(name: String, fieldType: FieldType) =
        writers[name]?.let {
            if ((it.field.type == fieldType.type
                        && (it.field.isNullable || !fieldType.isNullable))
                || it.field.type is UNION_TYPE
            )
                it
            else error("promotion no longer supported in StructVectorWriter")
        }
            ?: newChildWriter(name, fieldType)

    override fun endStruct() {
        vector.setIndexDefined(valueCount)
        val pos = ++valueCount
        writers.values.forEach { w -> w.populateWithAbsents(pos) }
    }

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is StructVector -> {
            if (src.field.isNullable && !field.isNullable)
                throw InvalidCopySourceException(src.field.fieldType, field.fieldType)

            val innerCopiers =
                src.map { child -> vectorFor(child.name, child.field.fieldType).rowCopier(child) }

            RowCopier { srcIdx ->
                valueCount.also {
                    if (src.isNull(srcIdx))
                        writeNull()
                    else {
                        innerCopiers.forEach { it.copyRow(srcIdx) }
                        endStruct()
                    }
                }
            }
        }

        else -> throw InvalidCopySourceException(src.field.fieldType, field.fieldType)
    }
}
