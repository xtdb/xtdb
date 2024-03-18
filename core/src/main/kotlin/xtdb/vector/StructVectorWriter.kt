package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.replaceChild
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.asKeyword
import xtdb.toFieldType
import xtdb.toLeg
import xtdb.util.normalForm
import xtdb.util.requiringResolve
import org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE as NULL_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Union as UNION_TYPE

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

    private fun IVectorWriter.promote(fieldType: FieldType): IVectorWriter {
        val structVector = this@StructVectorWriter.vector
        val al = structVector.allocator
        val field = field

        if (field.type is UNION_TYPE) return this

        syncValueCount()

        val newVec = when {
            fieldType.type == NULL_TYPE || (field.type == fieldType.type && fieldType.isNullable) ->
                vector
                    .getTransferPair(Field(field.name, FieldType.nullable(field.type), field.children), al)
                    .also { it.transfer() }
                    .to as FieldVector

            field.type == NULL_TYPE ->
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

        structVector.replaceChild(newVec)

        return writerFor(newVec).also {
            upsertChildField(it.field)
            writers[vector.name] = it
        }
    }

    private fun IVectorWriter.writeChildObject(v: Any?): IVectorWriter =
        try {
            if (v is IValueReader) writeValue(v) else writeObject(v)
            this
        } catch (e: InvalidWriteObjectException) {
            promote(e.obj.toFieldType()).also { promoted ->
                if (v is IValueReader) promoted.writeValue(v) else promoted.writeObject(v)
            }
        }

    override fun writeObject0(obj: Any) {
        if (obj !is Map<*, *>) throw InvalidWriteObjectException(field, obj)

        writeStruct {
            val structPos = wp.position

            for ((k, v) in obj) {
                val normalKey = normalForm(
                    when (k) {
                        is Keyword -> k.sym.toString()
                        is String -> k
                        else -> throw IllegalArgumentException("invalid struct key: '$k'")
                    }
                )

                val writer = writers[normalKey] ?: newChildWriter(normalKey, v.toFieldType())

                if (writer.writerPosition().position != structPos)
                    throw xtdb.IllegalArgumentException(
                        "xtdb/key-already-set".asKeyword,
                        data = mapOf("ks".asKeyword to obj.keys, "k".asKeyword to k)
                    )

                writer.writeChildObject(v)
            }
        }
    }

    private fun newChildWriter(key: String, fieldType: FieldType): IVectorWriter {
        val pos = wp.position
        val fieldType1 = if (pos == 0) fieldType else FieldType(true, fieldType.type, fieldType.dictionary)

        return writerFor(vector.addOrGet(key, fieldType1, FieldVector::class.java))
            .also {
                upsertChildField(it.field)
                writers[key] = it
                it.populateWithAbsents(pos)
            }
    }

    override fun structKeyWriter(key: String): IVectorWriter =
        writers[key] ?: newChildWriter(key, FieldType.nullable(NULL_TYPE))

    override fun structKeyWriter(key: String, fieldType: FieldType) =
        writers[key]?.let { if (it.field.fieldType == fieldType) it else it.promote(fieldType) }
            ?: newChildWriter(key, fieldType)

    override fun startStruct() = vector.setIndexDefined(wp.position)

    override fun endStruct() {
        val pos = ++wp.position
        writers.values.forEach { w ->
            try {
                w.populateWithAbsents(pos)
            } catch (e: InvalidWriteObjectException) {
                w.promote(FieldType.nullable(NULL_TYPE)).populateWithAbsents(pos)
            }
        }
    }

    private inline fun writeStruct(f: () -> Unit) {
        startStruct(); f(); endStruct()
    }

    private inline fun childRowCopier(
        srcName: String,
        fieldType: FieldType,
        toRowCopier: (IVectorWriter) -> IRowCopier,
    ): IRowCopier {
        val childWriter = structKeyWriter(srcName, fieldType)

        return try {
            toRowCopier(childWriter)
        } catch (e: InvalidCopySourceException) {
            return toRowCopier(childWriter.promote(e.src.fieldType))
        }
    }

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is StructVector -> {
            val innerCopiers =
                src.map { child -> childRowCopier(child.name, child.field.fieldType) { w -> w.rowCopier(child) } }

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

    override fun rowCopier(src: RelationReader): IRowCopier {
        val innerCopiers =
            src.map { child -> childRowCopier(child.name, child.field.fieldType) { w -> child.rowCopier(w) } }

        return IRowCopier { srcIdx ->
            wp.position.also {
                writeStruct {
                    innerCopiers.forEach { it.copyRow(srcIdx) }
                }

            }
        }
    }
}
