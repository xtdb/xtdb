package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.replaceChild
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.asKeyword
import xtdb.isSubType
import xtdb.toFieldType
import xtdb.toLeg
import xtdb.util.normalForm
import org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE as NULL_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Union as UNION_TYPE

class StructVectorWriter(override val vector: StructVector, override val notify: FieldChangeListener?) : IVectorWriter,
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

    private fun promoteChild(childWriter: IVectorWriter, fieldType: FieldType): IVectorWriter =
        if (childWriter.field.type is UNION_TYPE) childWriter
        else writerFor(childWriter.promote(fieldType, vector.allocator)).also {
            vector.replaceChild(it.vector)
            upsertChildField(it.field)
            writers[childWriter.vector.name] = it
        }

    private fun IVectorWriter.writeChildObject(v: Any?): IVectorWriter =
        try {
            if (v is IValueReader) writeValue(v) else writeObject(v)
            this
        } catch (e: InvalidWriteObjectException) {
            promoteChild(this, e.obj.toFieldType()).also { promoted ->
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
        writers[key]?.let {
            if (it.field.type == fieldType.type && (it.field.isNullable || !fieldType.isNullable)) it
            else promoteChild(it, fieldType)
        }
            ?: newChildWriter(key, fieldType)

    override fun startStruct() = vector.setIndexDefined(wp.position)

    override fun endStruct() {
        val pos = ++wp.position
        writers.values.forEach { w ->
            try {
                w.populateWithAbsents(pos)
            } catch (e: InvalidWriteObjectException) {
                promoteChild(w, FieldType.nullable(NULL_TYPE)).populateWithAbsents(pos)
            }
        }
    }

    private inline fun writeStruct(f: () -> Unit) {
        startStruct(); f(); endStruct()
    }

    override fun maybePromote(field: Field): IVectorWriter {
        return if (field.type == Struct.INSTANCE && (!field.isNullable || this.field.isNullable)) {
            for (potentialNewChild in field.children) {
                val newWriter : IVectorWriter = when (val childWriter = writers[potentialNewChild.name]) {
                    null -> newChildWriter(potentialNewChild.name, potentialNewChild.fieldType).maybePromote(potentialNewChild)
                    else ->
                        if (childWriter.field.type !is UNION_TYPE &&
                            ((potentialNewChild.type != childWriter.field.type) || ( potentialNewChild.isNullable && !childWriter.field.isNullable))) {
                            promoteChild(childWriter, potentialNewChild.fieldType).maybePromote(potentialNewChild)
                        } else {
                            childWriter.maybePromote(potentialNewChild)
                        }
                }
                upsertChildField(newWriter.field)
            }
            this
        } else {
            val newWriter = writerFor(promote(field.fieldType, vector.allocator), notify).also {
                if (it is DenseUnionVectorWriter) it.legWriter(field.type.toLeg()).maybePromote(field)
            }
            notify(newWriter.field)
            newWriter.writerPosition().position = writerPosition().position
            newWriter
        }
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
            return toRowCopier(promoteChild(childWriter, e.src.fieldType))
        }
    }

    override fun rowCopier(src: ValueVector) = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is StructVector -> {
            assert(isSubType(src.field, field))

            val innerCopiers =
                src.map { child -> childRowCopier(child.name, child.field.fieldType) { w -> w.rowCopier(child) } }.toMutableList()

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
