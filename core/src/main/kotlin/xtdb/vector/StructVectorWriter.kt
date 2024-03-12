package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.asKeyword
import xtdb.util.normalForm
import java.util.HashMap

class StructVectorWriter(override val vector: StructVector, private val notify: FieldChangeListener?) : IVectorWriter,
    Iterable<Map.Entry<String, IVectorWriter>> {
    private val wp = IVectorPosition.build(vector.valueCount)
    override var field: Field = vector.field

    private val childFields: MutableMap<String, Field> =
        field.children.associateByTo(HashMap()) { childField -> childField.name }

    override fun writerPosition() = wp

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

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())

    override fun writeNull() {
        super.writeNull()
        writers.values.forEach(IVectorWriter::writeNull)
    }

    override fun writeObject0(obj: Any) {
        if (obj !is Map<*, *>) throw InvalidWriteObjectException(field, obj)

        writeStruct {
            val structPos = wp.position

            for ((k, v) in obj) {
                val writer = structKeyWriter(
                    normalForm(
                        when (k) {
                            is Keyword -> k.sym.toString()
                            is String -> k
                            else -> throw IllegalArgumentException("invalid struct key: '$k'")
                        }
                    )
                )

                if (writer.writerPosition().position != structPos)
                    throw xtdb.IllegalArgumentException(
                        "xtdb/key-already-set".asKeyword,
                        data = mapOf("ks".asKeyword to obj.keys, "k".asKeyword to k)
                    )

                when (v) {
                    is IValueReader -> writer.writeValue(v)
                    else -> writer.writeObject(v)
                }
            }
        }
    }

    override fun structKeyWriter(key: String): IVectorWriter = writers[key] ?: structKeyWriter(key, UNION_FIELD_TYPE)

    override fun structKeyWriter(key: String, fieldType: FieldType): IVectorWriter {
        val w = writers[key]
        if (w != null) return w.also { it.checkFieldType(fieldType) }

        return writerFor(vector.addOrGet(key, fieldType, FieldVector::class.java))
            .also {
                upsertChildField(Field(key, fieldType, emptyList()))
                writers[key] = it
                it.populateWithAbsents(wp.position)
            }
    }

    override fun startStruct() = vector.setIndexDefined(wp.position)

    override fun endStruct() {
        val pos = wp.getPositionAndIncrement()
        writers.values.forEach { it.populateWithAbsents(pos + 1) }
    }

    private inline fun writeStruct(f: () -> Unit) {
        startStruct(); f(); endStruct()
    }

    override fun legWriter(leg: ArrowType) = monoLegWriter(leg)

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is StructVector -> {
            val innerCopiers = src.map { structKeyWriter(it.name).rowCopier(it) }
            IRowCopier { srcIdx ->
                wp.position.also {
                    if (src.isNull(srcIdx))
                        writeNull()
                    else writeStruct {
                        innerCopiers.forEach { it.copyRow(srcIdx) }
                    }
                }
            }
        }

        else -> throw InvalidCopySourceException(src.field, field)
    }
}
