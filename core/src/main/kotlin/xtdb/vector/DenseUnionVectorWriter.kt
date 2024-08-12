package xtdb.vector

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.replaceChild
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorPosition
import xtdb.toArrowType
import xtdb.toLeg
import java.nio.ByteBuffer

class DenseUnionVectorWriter(
    override val vector: DenseUnionVector,
    private val notify: FieldChangeListener?,
) : IVectorWriter {
    private val wp = VectorPosition.build(vector.valueCount)
    override var field: Field = vector.field

    private val childFields: MutableMap<String, Field> =
        field.children.associateByTo(LinkedHashMap()) { childField -> childField.name }

    private inner class ChildWriter(private val inner: IVectorWriter, val typeId: Byte) : IVectorWriter {
        override val vector get() = inner.vector
        override val field: Field get() = inner.field
        private val parentDuv get() = this@DenseUnionVectorWriter.vector
        private val parentWP get() = this@DenseUnionVectorWriter.wp

        override fun writerPosition() = inner.writerPosition()

        override fun clear() = inner.clear()

        private fun writeValue() {
            val pos = parentWP.getPositionAndIncrement()
            parentDuv.setTypeId(pos, typeId)
            parentDuv.setOffset(pos, this.writerPosition().position)
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

        override fun structKeyWriter(key: String) = inner.structKeyWriter(key)
        override fun structKeyWriter(key: String, fieldType: FieldType) = inner.structKeyWriter(key, fieldType)

        override fun startStruct() = inner.startStruct()

        override fun endStruct() {
            writeValue(); inner.endStruct()
        }

        override fun listElementWriter() = inner.listElementWriter()
        override fun listElementWriter(fieldType: FieldType) = inner.listElementWriter(fieldType)

        override fun startList() = inner.startList()

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

        override fun rowCopier(src: RelationReader): RowCopier {
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

    override fun writerPosition() = wp

    override fun clear() {
        super.clear()
        writersByLeg.values.forEach(IVectorWriter::clear)
    }

    override fun writeNull() {
        // DUVs can't technically contain null, but when they're stored within a nullable struct/list vector,
        // we don't have anything else to write here :/

        wp.getPositionAndIncrement()
    }

    override fun writeObject(obj: Any?): Unit = if (obj == null) legWriter(ArrowType.Null.INSTANCE).writeNull() else writeObject0(obj)
    override fun writeObject0(obj: Any): Unit = legWriter(obj.toArrowType()).writeObject0(obj)

    // DUV overrides the nullable one because DUVs themselves can't be null.
    override fun writeValue(v: ValueReader) {
        legWriter(v.leg!!).writeValue(v)
    }

    override fun writeValue0(v: ValueReader) = throw UnsupportedOperationException()

    private data class MissingLegException(val available: Set<String>, val requested: String) : NullPointerException()

    override fun legWriter(leg: String) =
        writersByLeg[leg] ?: throw MissingLegException(writersByLeg.keys, leg)

    private fun promoteLeg(legWriter: IVectorWriter, fieldType: FieldType): IVectorWriter {
        val typeId = (legWriter as ChildWriter).typeId

        return writerFor(legWriter.promote(fieldType, vector.allocator), typeId).also { newLegWriter ->
            vector.replaceChild(typeId, newLegWriter.vector)
            upsertChildField(newLegWriter.field)
            writersByLeg[newLegWriter.field.name] = newLegWriter
        }
    }

    @Suppress("NAME_SHADOWING")
    override fun legWriter(leg: String, fieldType: FieldType): IVectorWriter {
        val isNew = leg !in writersByLeg

        var w: IVectorWriter = writersByLeg.computeIfAbsent(leg) { leg ->
            val field = Field(leg, fieldType, emptyList())
            val typeId = vector.registerNewTypeId(field)

            val child = vector.addVector(typeId, fieldType.createNewSingleVector(field.name, vector.allocator, null))
            writerFor(child, typeId)
        }

        if (isNew) {
            upsertChildField(w.field)
        } else if(fieldType.isNullable && !w.field.isNullable) {
            w = promoteLeg(w, fieldType)
        }

        if (fieldType.type != w.field.type) {
            throw FieldMismatch(w.field.fieldType, fieldType)
        }

        return w
    }

    override fun legWriter(leg: ArrowType) = legWriter(leg.toLeg(), FieldType.notNullable(leg))

    private fun duvRowCopier(src: DenseUnionVector): RowCopier {
        val copierMapping = src.map { childVec ->
            val childField = childVec.field
            legWriter(childField.name, childField.fieldType).rowCopier(childVec)
        }

        return RowCopier { srcIdx ->
            copierMapping[src.getTypeId(srcIdx).also { check(it >= 0) }.toInt()]
                .copyRow(src.getOffset(srcIdx))
        }
    }

    override fun promoteChildren(field: Field) {
        if (field.type is ArrowType.Union) {
            for (child in field.children) {
                val legWriter = legWriter(child.type.toLeg(), child.fieldType)
                if (child.children.isNotEmpty()) legWriter.promoteChildren(child)
            }
        } else {
            val legWriter = legWriter(field.type.toLeg(), field.fieldType)
            if (field.children.isNotEmpty()) legWriter.promoteChildren(field)
        }
    }

    private fun rowCopier0(src: ValueVector): RowCopier {
        val srcField = src.field
        return legWriter(srcField.type.toLeg(), srcField.fieldType).rowCopier(src)
    }

    override fun rowCopier(src: ValueVector) =
        if (src is DenseUnionVector) duvRowCopier(src) else rowCopier0(src)
}
