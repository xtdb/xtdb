package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.toArrowType
import xtdb.toLeg
import java.nio.ByteBuffer


class SparseUnionVectorWriter(
    override val vector: UnionVector,
    private val notify: FieldChangeListener?,
) : IVectorWriter {
    private val wp = IVectorPosition.build(vector.valueCount)
    override var field: Field = vector.field

    private val childFields: MutableMap<String, Field> =
        field.children.associateByTo(LinkedHashMap()) { childField -> childField.name }

    private inner class ChildWriter(private val inner: IVectorWriter, val typeId: Byte) : IVectorWriter {
        override val vector get() = inner.vector
        override val field: Field get() = inner.field
        private val parentSuv get() = this@SparseUnionVectorWriter.vector
        private val parentWP get() = this@SparseUnionVectorWriter.wp

        override fun writerPosition() = inner.writerPosition()

        override fun clear() = inner.clear()

        private fun writeValue() {
            val pos = parentWP.getPositionAndIncrement()
            parentSuv.setTypeId(pos, typeId)
            inner.setWriterPosition(pos)
//            inner.writerPosition().position = pos
//            inner.syncValueCount()
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

        override fun startStruct()  {
            inner.setWriterPosition(parentWP.position);
            inner.startStruct()
        }

        override fun endStruct() {
            inner.endStruct()
            parentSuv.setTypeId(parentWP.getPositionAndIncrement(), typeId)
        }

        override fun listElementWriter() = inner.listElementWriter()
        override fun listElementWriter(fieldType: FieldType) = inner.listElementWriter(fieldType)


        override fun startList() {
            inner.setWriterPosition(parentWP.position)
            inner.startList()
        }

        override fun endList() {
            inner.endList()
            parentSuv.setTypeId(parentWP.getPositionAndIncrement(), typeId)
        }

        override fun writeValue0(v: IValueReader) {
            writeValue(); inner.writeValue0(v)
        }

        override fun rowCopier(src: ValueVector): IRowCopier {
            val innerCopier = inner.rowCopier(src)

            return IRowCopier { srcIdx ->
                writeValue()
                innerCopier.copyRow(srcIdx)
            }
        }

        override fun rowCopier(src: RelationReader): IRowCopier {
            val innerCopier = inner.rowCopier(src)

            return IRowCopier { srcIdx ->
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

    private fun writerFor(child: ValueVector, typeId: Byte) = ChildWriter(writerFor(child, ::upsertChildField), typeId)

    private val writersByLeg: MutableMap<Keyword, IVectorWriter> = vector.mapIndexed { typeId, child ->
        Keyword.intern(child.name) to writerFor(child, typeId.toByte())
    }.toMap(java.util.HashMap())

    override fun writerPosition() = wp

    override fun clear() {
        super.clear()
        writersByLeg.values.forEach(IVectorWriter::clear)
    }

    override fun writeNull() {
        // SUVs can't technically contain null, but when they're stored within a nullable struct/list vector,
        // we don't have anything else to write here :/

        wp.getPositionAndIncrement()
    }

    override fun writeObject(obj: Any?): Unit =
        if (obj == null) legWriter(ArrowType.Null.INSTANCE).writeNull() else writeObject0(obj)

    override fun writeObject0(obj: Any): Unit = legWriter(obj.toArrowType()).writeObject0(obj)

    // SUV overrides the nullable one because SUVs themselves can't be null.
    override fun writeValue(v: IValueReader) {
        legWriter(v.leg!!).writeValue(v)
    }

    override fun writeValue0(v: IValueReader) = throw UnsupportedOperationException()

    private data class MissingLegException(val available: Set<Keyword>, val requested: Keyword) : NullPointerException()

    override fun legWriter(leg: Keyword) =
        writersByLeg[leg] ?: throw MissingLegException(writersByLeg.keys, leg)

    private fun promoteLeg(legWriter: IVectorWriter, fieldType: FieldType): IVectorWriter {
        throw UnsupportedOperationException()
    }

    @Suppress("NAME_SHADOWING")
    override fun legWriter(leg: Keyword, fieldType: FieldType): IVectorWriter {
        // SUV legs should be nullable
        val fieldType = if (fieldType.isNullable)  FieldType.nullable(fieldType.type) else fieldType

        val isNew = leg !in writersByLeg

        var w: IVectorWriter = writersByLeg.computeIfAbsent(leg) { leg ->
            val field = Field(leg.sym.name, fieldType, emptyList())
            // just taking the next TypeId for now
            val typeId =  writersByLeg.size.toByte()

//            var child = fieldType.createNewSingleVector(field.name, vector.allocator, null)
//            vector.directAddVector(child)
            var child =  vector.addVector(fieldType.createNewSingleVector(field.name, vector.allocator, null))
            writerFor(child, typeId)
        }

        if (isNew) {
            upsertChildField(w.field)
        }

        if (fieldType.type != w.field.type) {
            throw FieldMismatch(w.field.fieldType, fieldType)
        }

        return w
    }

    override fun legWriter(leg: ArrowType) = legWriter(leg.toLeg(), FieldType.notNullable(leg))

    private fun suvRowCopier(src: UnionVector): IRowCopier {
        val copierMapping = src.map { childVec ->
            val childField = childVec.field
            legWriter(Keyword.intern(childField.name), childField.fieldType).rowCopier(childVec)
        }

        return IRowCopier { srcIdx ->
            copierMapping[src.getTypeValue(srcIdx).also { check(it >= 0) }.toInt()].copyRow(srcIdx)
        }
    }

    private fun rowCopier0(src: ValueVector): IRowCopier {
        val srcField = src.field
        return legWriter(srcField.type.toLeg(), srcField.fieldType).rowCopier(src)
    }

    override fun rowCopier(src: ValueVector) = when (src) {
        is UnionVector -> suvRowCopier(src)
        // TODO
        is DenseUnionVector -> throw UnsupportedOperationException()
        else -> rowCopier0(src)
    }
}