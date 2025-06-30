package xtdb.vector

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.replaceChild
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.NULL_TYPE
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.toArrowType
import xtdb.toLeg
import java.nio.ByteBuffer

class DenseUnionVectorWriter(
    override val vector: DenseUnionVector,
    private val notify: FieldChangeListener?,
) : IVectorWriter {
    override var valueCount = vector.valueCount
    override var field: Field = vector.field

    private val childFields: MutableMap<String, Field> =
        field.children.associateByTo(LinkedHashMap()) { childField -> childField.name }

    private inner class ChildWriter(private val inner: IVectorWriter, val typeId: Byte) : IVectorWriter {
        override val vector get() = inner.vector
        override val field: Field get() = inner.field
        private val parentDuv get() = this@DenseUnionVectorWriter.vector

        override var valueCount: Int
            get() = inner.valueCount
            set(value) {
                inner.valueCount = value
            }

        override fun clear() = inner.clear()

        private fun writeValue() {
            val pos = this@DenseUnionVectorWriter.valueCount++
            parentDuv.setTypeId(pos, typeId)
            parentDuv.setOffset(pos, valueCount)
        }

        override fun writeNull() {
            writeValue(); inner.writeNull()
        }

        override fun writeBoolean(v: Boolean) {
            writeValue(); inner.writeBoolean(v)
        }

        override fun writeByte(v: Byte) {
            writeValue(); inner.writeByte(v)
        }

        override fun writeShort(v: Short) {
            writeValue(); inner.writeShort(v)
        }

        override fun writeInt(v: Int) {
            writeValue(); inner.writeInt(v)
        }

        override fun writeLong(v: Long) {
            writeValue(); inner.writeLong(v)
        }

        override fun writeFloat(v: Float) {
            writeValue(); inner.writeFloat(v)
        }

        override fun writeDouble(v: Double) {
            writeValue(); inner.writeDouble(v)
        }

        override fun writeBytes(v: ByteBuffer) {
            writeValue(); inner.writeBytes(v)
        }

        override fun writeObject0(obj: Any) {
            writeValue(); inner.writeObject0(obj)
        }

        override fun vectorFor(name: String, fieldType: FieldType) = inner.vectorFor(name, fieldType)

        override fun endStruct() {
            writeValue(); inner.endStruct()
        }

        override val listElements get() = inner.listElements

        override fun getListElements(fieldType: FieldType) = inner.getListElements(fieldType)

        override fun endList() {
            writeValue(); inner.endList()
        }

        override fun writeValue0(v: ValueReader) {
            writeValue(); inner.writeValue0(v)
        }

        override fun rowCopier(src: ValueVector): RowCopier {
            val innerCopier = inner.rowCopier(src)

            return RowCopier { srcIdx ->
                writeValue()
                innerCopier.copyRow(srcIdx)
            }
        }

    }

    private fun upsertChildField(childField: Field) {
        childFields[childField.name] = childField
        field = Field(field.name, field.fieldType, childFields.values.toList())
        notify(field)
    }

    private fun writerFor(child: ValueVector, typeId: Byte) =
        ChildWriter(writerFor(child, ::upsertChildField), typeId)

    private val writersByLeg: MutableMap<String, IVectorWriter> = vector.mapIndexed { typeId, child ->
        child.name to writerFor(child, typeId.toByte())
    }.toMap(HashMap())

    override fun clear() {
        super.clear()
        writersByLeg.values.forEach(IVectorWriter::clear)
    }

    override fun writeNull() {
        // DUVs can't technically contain null, but when they're stored within a nullable struct/list vector,
        // we don't have anything else to write here :/

        valueCount++
    }

    override fun writeObject(obj: Any?): Unit =
        if (obj == null) vectorFor(NULL_TYPE.toLeg(), FieldType.nullable(NULL_TYPE)).writeNull() else writeObject0(obj)

    override fun writeObject0(obj: Any) {
        val leg = obj.toArrowType()
        vectorFor(leg.toLeg(), FieldType.notNullable(leg)).writeObject0(obj)
    }

    // DUV overrides the nullable one because DUVs themselves can't be null.
    override fun writeValue(v: ValueReader) {
        vectorFor(v.leg!!).writeValue(v)
    }

    override fun writeValue0(v: ValueReader) = throw UnsupportedOperationException()

    override fun vectorForOrNull(name: String) = writersByLeg[name]

    private fun promoteLeg(legWriter: IVectorWriter, fieldType: FieldType): IVectorWriter {
        val typeId = (legWriter as ChildWriter).typeId

        return writerFor(legWriter.promote(fieldType, vector.allocator), typeId).also { newLegWriter ->
            vector.replaceChild(typeId, newLegWriter.vector)
            upsertChildField(newLegWriter.field)
            writersByLeg[newLegWriter.field.name] = newLegWriter
        }
    }

    override fun vectorFor(name: String, fieldType: FieldType): IVectorWriter {
        val isNew = name !in writersByLeg

        var w: IVectorWriter = writersByLeg.computeIfAbsent(name) { leg ->
            val field = Field(leg, fieldType, emptyList())
            val typeId = vector.registerNewTypeId(field)

            val child = vector.addVector(typeId, fieldType.createNewSingleVector(field.name, vector.allocator, null))
            writerFor(child, typeId)
        }

        if (isNew) {
            upsertChildField(w.field)
        } else if (fieldType.isNullable && !w.field.isNullable) {
            w = promoteLeg(w, fieldType)
        }

        if (fieldType.type != w.field.type) {
            throw FieldMismatch(w.field.fieldType, fieldType)
        }

        return w
    }

    private fun duvRowCopier(src: DenseUnionVector): RowCopier {
        val copierMapping = src.map { childVec ->
            val childField = childVec.field
            vectorFor(childField.name, childField.fieldType).rowCopier(childVec)
        }

        return RowCopier { srcIdx ->
            val typeId = src.getTypeId(srcIdx)
            if (typeId < 0)
                writeUndefined()
            else
                copierMapping[src.getTypeId(srcIdx).toInt()]
                    .copyRow(src.getOffset(srcIdx))
        }
    }

    override fun promoteChildren(field: Field) {
        if (field.type is ArrowType.Union) {
            for (child in field.children) {
                val legWriter = vectorFor(child.type.toLeg(), child.fieldType)
                if (child.children.isNotEmpty()) legWriter.promoteChildren(child)
            }
        } else {
            val legWriter = vectorFor(field.type.toLeg(), field.fieldType)
            if (field.children.isNotEmpty()) legWriter.promoteChildren(field)
        }
    }

    private fun rowCopier0(src: ValueVector): RowCopier {
        val srcField = src.field
        return vectorFor(srcField.type.toLeg(), srcField.fieldType).rowCopier(src)
    }

    override fun rowCopier(src: ValueVector) =
        if (src is DenseUnionVector) duvRowCopier(src) else rowCopier0(src)
}
