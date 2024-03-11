@file:JvmName("FieldVectorWriters")

package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.*
import org.apache.arrow.vector.compare.VectorVisitor
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.Union
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.RuntimeException
import xtdb.asKeyword
import xtdb.toArrowType
import xtdb.toLeg
import xtdb.types.ClojureForm
import xtdb.types.IntervalDayTime
import xtdb.types.IntervalMonthDayNano
import xtdb.types.IntervalYearMonth
import xtdb.util.normalForm
import xtdb.util.requiringResolve
import xtdb.vector.extensions.*
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.time.*
import java.time.ZoneOffset.UTC
import java.util.*
import kotlin.text.Charsets.UTF_8
import org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE as NULL_TYPE

private val UNION_FIELD_TYPE = FieldType.notNullable(Union(UnionMode.Dense, null))

fun interface FieldChangeListener {
    fun notify(f: Field)
}

private operator fun FieldChangeListener?.invoke(f: Field) = this?.notify(f)

private fun nullToVecCopier(dest: IVectorWriter): IRowCopier {
    val wp = dest.writerPosition()
    return IRowCopier { _ -> wp.position.also { dest.writeNull() } }
}

private fun duvToVecCopier(dest: IVectorWriter, src: DenseUnionVector): IRowCopier {
    val copiers = src.map { dest.rowCopier(it) }
    return IRowCopier { srcIdx -> copiers[src.getTypeId(srcIdx).toInt()].copyRow(src.getOffset(srcIdx)) }
}

private fun IVectorWriter.monoLegWriter(leg: ArrowType): IVectorWriter {
    if ((leg == NULL_TYPE && field.isNullable) || leg == field.type) return this
    throw IllegalArgumentException("arrow-type mismatch: got <${field.type}>, requested <$leg>")
}

private data class InvalidWriteObjectException(val field: Field, val obj: Any?) :
    IllegalArgumentException("invalid writeObject")

private data class InvalidCopySourceException(val src: Field, val dest: Field) :
    IllegalArgumentException("illegal copy src vector")

abstract class ScalarVectorWriter(vector: FieldVector) : IVectorWriter {

    protected val wp = IVectorPosition.build(vector.valueCount)

    override val field: Field get() = vector.field

    override fun writerPosition() = wp

    override fun legWriter(leg: ArrowType) = monoLegWriter(leg)

    override fun rowCopier(src: ValueVector): IRowCopier {
        return when {
            src is NullVector -> nullToVecCopier(this)
            src is DenseUnionVector -> duvToVecCopier(this, src)
            src.javaClass != vector.javaClass -> throw InvalidCopySourceException(src.field, field)
            else -> IRowCopier { srcIdx ->
                wp.getPositionAndIncrement().also { pos ->
                    vector.copyFromSafe(srcIdx, pos, src)
                }
            }
        }
    }
}

class NullVectorWriter(override val vector: NullVector) : ScalarVectorWriter(vector) {
    override fun writeValue0(v: IValueReader) = writeNull()
    override fun writeObject0(obj: Any): Unit = throw UnsupportedOperationException()
    override fun rowCopier(src: ValueVector) = IRowCopier { _ -> wp.position.also { writeNull() } }
}

private class BitVectorWriter(override val vector: BitVector) : ScalarVectorWriter(vector) {
    override fun writeBoolean(v: Boolean) = vector.setSafe(wp.getPositionAndIncrement(), if (v) 1 else 0)
    override fun writeObject0(obj: Any) = writeBoolean(obj as? Boolean ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeBoolean(v.readBoolean())
}

private class TinyIntVectorWriter(override val vector: TinyIntVector) : ScalarVectorWriter(vector) {
    override fun writeByte(v: Byte) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeByte(obj as? Byte ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeByte(v.readByte())
}

private class SmallIntVectorWriter(override val vector: SmallIntVector) : ScalarVectorWriter(vector) {
    override fun writeShort(v: Short) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeShort(obj as? Short ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeShort(v.readShort())
}

private class IntVectorWriter(override val vector: IntVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeInt(obj as? Int ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeInt(v.readInt())
}

private class BigIntVectorWriter(override val vector: BigIntVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeLong(obj as? Long ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class Float4VectorWriter(override val vector: Float4Vector) : ScalarVectorWriter(vector) {
    override fun writeFloat(v: Float) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeFloat(obj as? Float ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeFloat(v.readFloat())
}

private class Float8VectorWriter(override val vector: Float8Vector) : ScalarVectorWriter(vector) {
    override fun writeDouble(v: Double) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeDouble(obj as? Double ?: throw InvalidWriteObjectException(field, obj))
    override fun writeValue0(v: IValueReader) = writeDouble(v.readDouble())
}

private class DateDayVectorWriter(override val vector: DateDayVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = when (obj) {
        is LocalDate -> writeInt(obj.toEpochDay().toInt())
        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeInt(v.readInt())
}

private class DateMilliVectorWriter(override val vector: DateMilliVector) : ScalarVectorWriter(vector) {
    // `v` in days here
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v * 86_400_000)

    override fun writeObject0(obj: Any) = when (obj) {
        is LocalDate -> writeLong(obj.toEpochDay())
        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private fun TimeUnit.toLong(seconds: Long, nanos: Int): Long = when (this) {
    TimeUnit.SECOND -> seconds
    TimeUnit.MILLISECOND -> seconds * 1000 + nanos / 1_000_000
    TimeUnit.MICROSECOND -> seconds * 1_000_000 + nanos / 1000
    TimeUnit.NANOSECOND -> seconds * 1_000_000_000 + nanos
}

private class TimestampVectorWriter(override val vector: TimeStampVector) : ScalarVectorWriter(vector) {
    private val unit: TimeUnit = (vector.field.type as ArrowType.Timestamp).unit

    private fun Date.toLong(): Long = when (unit) {
        TimeUnit.SECOND -> time / 1000
        TimeUnit.MILLISECOND -> time
        TimeUnit.MICROSECOND -> time * 1000
        TimeUnit.NANOSECOND -> time * 1_000_000
    }

    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)

    override fun writeObject0(obj: Any): Unit = when (obj) {
        is Date -> writeLong(obj.toLong())
        is Instant -> writeLong(unit.toLong(obj.epochSecond, obj.nano))
        is ZonedDateTime -> writeObject0(obj.toInstant())
        is OffsetDateTime -> writeObject0(obj.toInstant())
        is LocalDateTime -> writeObject0(obj.toInstant(UTC))
        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private abstract class TimeVectorWriter(vector: FieldVector) : ScalarVectorWriter(vector) {
    private val multiplier: Long = when ((vector.field.type as ArrowType.Time).unit!!) {
        TimeUnit.SECOND -> 1_000_000_000
        TimeUnit.MILLISECOND -> 1_000_000
        TimeUnit.MICROSECOND -> 1_000
        TimeUnit.NANOSECOND -> 1
    }

    override fun writeObject0(obj: Any): Unit = when (obj) {
        is LocalTime -> writeLong(obj.toNanoOfDay() * multiplier)
        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class TimeSecVectorWriter(override val vector: TimeSecVector) : TimeVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v.toInt())
}

private class TimeMilliVectorWriter(override val vector: TimeMilliVector) : TimeVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v.toInt())
}

private class TimeMicroVectorWriter(override val vector: TimeMicroVector) : TimeVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
}

private class TimeNanoVectorWriter(override val vector: TimeNanoVector) : TimeVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
}

private class DecimalVectorWriter(override val vector: DecimalVector) : ScalarVectorWriter(vector) {
    override fun writeObject0(obj: Any) {
        if (obj !is BigDecimal) throw InvalidWriteObjectException(field, obj)
        vector.setSafe(wp.getPositionAndIncrement(), obj.setScale(vector.scale))
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class DurationVectorWriter(override val vector: DurationVector) : ScalarVectorWriter(vector) {
    private val unit: TimeUnit = (vector.field.type as ArrowType.Duration).unit

    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any): Unit = when (obj) {
        is Duration -> writeLong(unit.toLong(obj.seconds, obj.nano))
        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private operator fun PeriodDuration.component1() = period
private operator fun PeriodDuration.component2() = duration

private class IntervalYearVectorWriter(override val vector: IntervalYearVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)

    override fun writeObject0(obj: Any): Unit = when (obj) {
        is PeriodDuration -> writeInt(obj.period.toTotalMonths().toInt())
        is IntervalYearMonth -> writeObject(PeriodDuration(obj.period, Duration.ZERO))
        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class IntervalDayVectorWriter(override val vector: IntervalDayVector) : ScalarVectorWriter(vector) {
    override fun writeObject0(obj: Any): Unit = when (obj) {
        is PeriodDuration -> {
            val (p, d) = obj
            require(p.years == 0 && p.months == 0)

            vector.setSafe(
                wp.getPositionAndIncrement(),
                p.days,
                Math.addExact(Math.multiplyExact(d.seconds, 1_000).toInt(), d.toMillisPart())
            )
        }

        is IntervalDayTime -> writeObject0(PeriodDuration(obj.period, obj.duration))

        else -> throw InvalidWriteObjectException(field, obj)
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class IntervalMdnVectorWriter(override val vector: IntervalMonthDayNanoVector) : ScalarVectorWriter(vector) {
    override fun writeObject0(obj: Any): Unit = when (obj) {
        is PeriodDuration -> {
            val (p, d) = obj

            vector.setSafe(
                wp.getPositionAndIncrement(),
                p.toTotalMonths().toInt(),
                p.days,
                Math.addExact(Math.multiplyExact(d.seconds, 1_000_000_000), d.toNanosPart().toLong())
            )
        }

        is IntervalMonthDayNano -> writeObject0(PeriodDuration(obj.period, obj.duration))

        else -> throw InvalidWriteObjectException(field, obj)
    }


    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class FixedSizeBinaryVectorWriter(override val vector: FixedSizeBinaryVector) : ScalarVectorWriter(vector) {
    override fun writeBytes(v: ByteBuffer) {
        val pos = v.position()

        val idx = wp.getPositionAndIncrement()
        vector.setIndexDefined(idx)
        vector.dataBuffer.setBytes((idx * vector.byteWidth).toLong(), v)

        v.position(pos)
    }

    override fun writeObject0(obj: Any) = writeBytes(when (obj) {
        is ByteBuffer -> obj
        is ByteArray -> ByteBuffer.wrap(obj)

        is UUID -> {
            ByteBuffer.allocate(16).also {
                it.putLong(obj.mostSignificantBits)
                it.putLong(obj.leastSignificantBits)
                it.position(0)
            }
        }

        else -> throw InvalidWriteObjectException(field, obj)
    })

    override fun writeValue0(v: IValueReader) = writeBytes(v.readBytes())
}

abstract class VariableWidthVectorWriter(vector: BaseVariableWidthVector) : ScalarVectorWriter(vector) {
    abstract override val vector: BaseVariableWidthVector

    override fun writeBytes(v: ByteBuffer) {
        val bufPos = v.position()
        vector.setSafe(wp.getPositionAndIncrement(), v, v.position(), v.remaining())
        v.position(bufPos)
    }

    override fun writeValue0(v: IValueReader) = writeBytes(v.readBytes())
}

private class VarCharVectorWriter(override val vector: VarCharVector) : VariableWidthVectorWriter(vector) {

    override fun writeObject0(obj: Any) {
        val str: CharSequence = when (obj) {
            is Keyword -> obj.sym.toString()
            else -> obj.toString()
        }

        writeBytes(UTF_8.newEncoder().encode(CharBuffer.wrap(str)))
    }
}

private class VarBinaryVectorWriter(override val vector: VarBinaryVector) : VariableWidthVectorWriter(vector) {
    override fun writeObject0(obj: Any) = writeBytes(
        when (obj) {
            is ByteBuffer -> obj
            is ByteArray -> ByteBuffer.wrap(obj)
            is ClojureForm, is RuntimeException, is xtdb.IllegalArgumentException ->
                ByteBuffer.wrap(requiringResolve("xtdb.serde/write-transit")(obj) as ByteArray)

            else -> throw InvalidWriteObjectException(field, obj)
        }
    )

    override fun writeValue0(v: IValueReader) = writeBytes(v.readBytes())
}

class ExtensionVectorWriter(override val vector: XtExtensionVector<*>, private val inner: IVectorWriter) :
    ScalarVectorWriter(vector) {

    override fun clear() = inner.clear()
    override fun writerPosition() = inner.writerPosition()

    override fun writeNull() = inner.writeNull()
    override fun writeByte(v: Byte) = inner.writeByte(v)
    override fun writeShort(v: Short) = inner.writeShort(v)
    override fun writeInt(v: Int) = inner.writeInt(v)
    override fun writeLong(v: Long) = inner.writeLong(v)
    override fun writeFloat(v: Float) = inner.writeFloat(v)
    override fun writeDouble(v: Double) = inner.writeDouble(v)
    override fun writeBytes(v: ByteBuffer) = inner.writeBytes(v)
    override fun writeObject0(obj: Any) = inner.writeObject(obj)
    override fun writeValue0(v: IValueReader) = inner.writeValue(v)

    override fun structKeyWriter(key: String) = inner.structKeyWriter(key)
    override fun structKeyWriter(key: String, fieldType: FieldType) = inner.structKeyWriter(key, fieldType)
    override fun startStruct() = inner.startStruct()
    override fun endStruct() = inner.endStruct()

    override fun listElementWriter() = inner.listElementWriter()
    override fun listElementWriter(fieldType: FieldType) = inner.listElementWriter(fieldType)
    override fun startList() = inner.startList()
    override fun endList() = inner.endList()

    override fun rowCopier(src: ValueVector): IRowCopier = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is XtExtensionVector<*> -> inner.rowCopier(src.underlyingVector)
        else -> throw InvalidCopySourceException(src.field, field)
    }
}

private data class FieldMismatch(val expected: FieldType, val given: FieldType) :
    IllegalArgumentException("Field type mismatch")

private fun IVectorWriter.checkFieldType(given: FieldType) {
    val expected = field.fieldType
    if (!((expected.type == NULL_TYPE && given.type == NULL_TYPE) || (expected == given)))
        throw FieldMismatch(expected, given)
}

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

private data class PopulateWithAbsentsException(val field: Field, val expectedPos: Int, val actualPos: Int) :
    IllegalStateException("populate-with-absents needs a nullable or a union underneath")

private fun IVectorWriter.populateWithAbsents(pos: Int) {
    val absents = pos - writerPosition().position
    if (absents > 0) {
        val field = this.field
        val absentWriter = when {
            field.type == UNION_FIELD_TYPE.type -> legWriter(AbsentType)
            field.isNullable -> this
            else -> throw PopulateWithAbsentsException(field, pos, writerPosition().position)
        }

        repeat(absents) { absentWriter.writeNull() }
    }
}

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

class DenseUnionVectorWriter(
    override val vector: DenseUnionVector,
    private val notify: FieldChangeListener?,
) : IVectorWriter {
    private val wp = IVectorPosition.build(vector.valueCount)
    override var field: Field = vector.field

    private val childFields: MutableMap<String, Field> =
        field.children.associateByTo(LinkedHashMap()) { childField -> childField.name }

    private inner class ChildWriter(private val inner: IVectorWriter, private val typeId: Byte) : IVectorWriter {
        override val vector get() = inner.vector
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

        override fun legWriter(leg: ArrowType) = inner.legWriter(leg)
        override fun legWriter(leg: Keyword) = inner.legWriter(leg)
        override fun legWriter(leg: Keyword, fieldType: FieldType) = inner.legWriter(leg, fieldType)
    }

    private fun upsertChildField(childField: Field) {
        childFields[childField.name] = childField
        field = Field(field.name, field.fieldType, childFields.values.toList())
        notify(field)
    }

    private fun writerFor(child: ValueVector, typeId: Byte) =
        ChildWriter(writerFor(child, ::upsertChildField), typeId)

    private val writersByLeg: MutableMap<Keyword, IVectorWriter> = vector.mapIndexed { typeId, child ->
        Keyword.intern(child.name) to writerFor(child, typeId.toByte())
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

    override fun writeObject(obj: Any?): Unit = if (obj == null) legWriter(NULL_TYPE).writeNull() else writeObject0(obj)
    override fun writeObject0(obj: Any): Unit = legWriter(obj.toArrowType()).writeObject0(obj)

    // DUV overrides the nullable one because DUVs themselves can't be null.
    override fun writeValue(v: IValueReader) {
        legWriter(v.leg!!).writeValue(v)
    }

    override fun writeValue0(v: IValueReader) = throw UnsupportedOperationException()

    private data class MissingLegException(val available: Set<Keyword>, val requested: Keyword) : NullPointerException()

    override fun legWriter(leg: Keyword) =
        writersByLeg[leg] ?: throw MissingLegException(writersByLeg.keys, leg)

    @Suppress("NAME_SHADOWING")
    override fun legWriter(leg: Keyword, fieldType: FieldType): IVectorWriter {
        val isNew = leg !in writersByLeg

        val w: IVectorWriter = writersByLeg.computeIfAbsent(leg) { leg ->
            val field = Field(leg.sym.name, fieldType, emptyList())
            val typeId = vector.registerNewTypeId(field)

            val child = vector.addVector(typeId, fieldType.createNewSingleVector(field.name, vector.allocator, null))
            writerFor(child, typeId)
        }

        if (isNew) {
            upsertChildField(w.field)
        } else {
            w.checkFieldType(fieldType)
        }

        return w
    }

    override fun legWriter(leg: ArrowType) = legWriter(leg.toLeg(), FieldType.notNullable(leg))

    private fun duvRowCopier(src: DenseUnionVector): IRowCopier {
        val copierMapping = src.map { childVec ->
            val childField = childVec.field
            legWriter(Keyword.intern(childField.name), childField.fieldType).rowCopier(childVec)
        }

        return IRowCopier { srcIdx ->
            copierMapping[src.getTypeId(srcIdx).also { check(it >= 0) }.toInt()]
                .copyRow(src.getOffset(srcIdx))
        }
    }

    private fun rowCopier0(src: ValueVector): IRowCopier {
        val srcField = src.field
        val isNullable = srcField.isNullable

        return object : IRowCopier {
            private val notNullCopier by lazy { legWriter(srcField.type).rowCopier(src) }
            private val nullCopier by lazy { legWriter(NULL_TYPE).rowCopier(src) }

            override fun copyRow(sourceIdx: Int): Int {
                return if (isNullable && src.isNull(sourceIdx)) {
                    nullCopier.copyRow(sourceIdx)
                } else {
                    notNullCopier.copyRow(sourceIdx)
                }
            }
        }
    }

    override fun rowCopier(src: ValueVector) =
        if (src is DenseUnionVector) duvRowCopier(src) else rowCopier0(src)
}

private object WriterForVectorVisitor : VectorVisitor<IVectorWriter, FieldChangeListener?> {
    override fun visit(vec: BaseFixedWidthVector, notify: FieldChangeListener?) = when (vec) {
        is BitVector -> BitVectorWriter(vec)
        is TinyIntVector -> TinyIntVectorWriter(vec)
        is SmallIntVector -> SmallIntVectorWriter(vec)
        is IntVector -> IntVectorWriter(vec)
        is BigIntVector -> BigIntVectorWriter(vec)
        is Float4Vector -> Float4VectorWriter(vec)
        is Float8Vector -> Float8VectorWriter(vec)

        is DecimalVector -> DecimalVectorWriter(vec)

        is DateDayVector -> DateDayVectorWriter(vec)
        is DateMilliVector -> DateMilliVectorWriter(vec)

        is TimeStampVector -> TimestampVectorWriter(vec)

        is TimeSecVector -> TimeSecVectorWriter(vec)
        is TimeMilliVector -> TimeMilliVectorWriter(vec)
        is TimeMicroVector -> TimeMicroVectorWriter(vec)
        is TimeNanoVector -> TimeNanoVectorWriter(vec)

        is DurationVector -> DurationVectorWriter(vec)

        is IntervalYearVector -> IntervalYearVectorWriter(vec)
        is IntervalDayVector -> IntervalDayVectorWriter(vec)
        is IntervalMonthDayNanoVector -> IntervalMdnVectorWriter(vec)

        is FixedSizeBinaryVector -> FixedSizeBinaryVectorWriter(vec)

        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }

    override fun visit(vec: BaseVariableWidthVector, notify: FieldChangeListener?) = when (vec) {
        is VarCharVector -> VarCharVectorWriter(vec)
        is VarBinaryVector -> VarBinaryVectorWriter(vec)

        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }

    override fun visit(vec: BaseLargeVariableWidthVector, notify: FieldChangeListener?): IVectorWriter =
        throw UnsupportedOperationException()

    override fun visit(vec: ListVector, notify: FieldChangeListener?) = ListVectorWriter(vec, notify)
    override fun visit(vec: FixedSizeListVector, notify: FieldChangeListener?): IVectorWriter =
        throw UnsupportedOperationException()

    override fun visit(vec: LargeListVector, notify: FieldChangeListener?): IVectorWriter =
        throw UnsupportedOperationException()

    override fun visit(vec: NonNullableStructVector, notify: FieldChangeListener?): IVectorWriter = when (vec) {
        is StructVector -> StructVectorWriter(vec, notify)
        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }

    override fun visit(vec: UnionVector, notify: FieldChangeListener?): IVectorWriter =
        throw UnsupportedOperationException()

    override fun visit(vec: DenseUnionVector, notify: FieldChangeListener?): IVectorWriter =
        DenseUnionVectorWriter(vec, notify)

    override fun visit(vec: NullVector, notify: FieldChangeListener?) = NullVectorWriter(vec)

    override fun visit(vec: ExtensionTypeVector<*>, notify: FieldChangeListener?) = when (vec) {
        is KeywordVector -> ExtensionVectorWriter(vec, VarCharVectorWriter(vec.underlyingVector))
        is UuidVector -> ExtensionVectorWriter(vec, FixedSizeBinaryVectorWriter(vec.underlyingVector))
        is UriVector -> ExtensionVectorWriter(vec, VarCharVectorWriter(vec.underlyingVector))
        is TransitVector -> ExtensionVectorWriter(vec, VarBinaryVectorWriter(vec.underlyingVector))
        is AbsentVector -> ExtensionVectorWriter(vec, NullVectorWriter(vec.underlyingVector))
        is SetVector -> ExtensionVectorWriter(vec, ListVectorWriter(vec.underlyingVector) { notify(vec.field) })
        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }
}

@JvmOverloads
fun writerFor(vec: ValueVector, notify: FieldChangeListener? = null): IVectorWriter =
    vec.accept(WriterForVectorVisitor, notify)

