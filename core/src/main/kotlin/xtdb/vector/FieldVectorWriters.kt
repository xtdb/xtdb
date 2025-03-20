@file:JvmName("FieldVectorWriters")

package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.vector.*
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.DateMilliVector
import org.apache.arrow.vector.DurationVector
import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.IntervalMonthDayNanoVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.compare.VectorVisitor
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.RuntimeException
import xtdb.arrow.*
import xtdb.types.*
import xtdb.util.requiringResolve
import xtdb.vector.extensions.*
import xtdb.vector.extensions.KeywordVector
import xtdb.vector.extensions.SetVector
import xtdb.vector.extensions.TransitVector
import xtdb.vector.extensions.TsTzRangeVector
import xtdb.vector.extensions.UuidVector
import java.math.BigDecimal
import java.net.URI
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.time.*
import java.time.ZoneOffset.UTC
import java.util.*
import kotlin.text.Charsets.UTF_8

fun interface FieldChangeListener {
    fun notify(f: Field)
}

internal operator fun FieldChangeListener?.invoke(f: Field) = this?.notify(f)

internal fun nullToVecCopier(dest: IVectorWriter): RowCopier {
    val wp = dest.writerPosition()
    return RowCopier { _ -> wp.position.also { dest.writeNull() } }
}

internal fun duvToVecCopier(dest: IVectorWriter, src: DenseUnionVector): RowCopier {
    val copiers = src.map { dest.rowCopier(it) }
    return RowCopier { srcIdx -> copiers[src.getTypeId(srcIdx).toInt()].copyRow(src.getOffset(srcIdx)) }
}

abstract class ScalarVectorWriter(vector: FieldVector) : IVectorWriter {

    protected val wp = VectorPosition.build(vector.valueCount)

    override val field: Field = vector.field

    override fun writerPosition() = wp

    override fun rowCopier(src: ValueVector): RowCopier {
        return when {
            src is NullVector -> nullToVecCopier(this)
            src is DenseUnionVector -> duvToVecCopier(this, src)
            src.javaClass != vector.javaClass || (src.field.isNullable && !field.isNullable) ->
                throw InvalidCopySourceException(src.field.fieldType, field.fieldType)

            else -> RowCopier { srcIdx ->
                wp.getPositionAndIncrement().also { pos ->
                    vector.copyFromSafe(srcIdx, pos, src)
                }
            }
        }
    }
}

class NullVectorWriter(override val vector: NullVector) : ScalarVectorWriter(vector) {
    override fun writeValue0(v: ValueReader) = writeNull()
    override fun writeObject0(obj: Any): Unit = throw InvalidWriteObjectException(field.fieldType, obj)
    override fun rowCopier(src: ValueVector) = when (src) {
        is DenseUnionVector -> duvToVecCopier(this, src)
        is NullVector -> RowCopier { _ -> wp.position.also { writeNull() } }
        else -> throw InvalidCopySourceException(src.field.fieldType, field.fieldType)
    }
}

private class BitVectorWriter(override val vector: BitVector) : ScalarVectorWriter(vector) {
    override fun writeBoolean(v: Boolean) = vector.setSafe(wp.getPositionAndIncrement(), if (v) 1 else 0)
    override fun writeObject0(obj: Any) = writeBoolean(obj as? Boolean ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeBoolean(v.readBoolean())
}

private class TinyIntVectorWriter(override val vector: TinyIntVector) : ScalarVectorWriter(vector) {
    override fun writeByte(v: Byte) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeByte(obj as? Byte ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeByte(v.readByte())
}

private class SmallIntVectorWriter(override val vector: SmallIntVector) : ScalarVectorWriter(vector) {
    override fun writeShort(v: Short) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeShort(obj as? Short ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeShort(v.readShort())
}

private class IntVectorWriter(override val vector: IntVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeInt(obj as? Int ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeInt(v.readInt())
}

private class BigIntVectorWriter(override val vector: BigIntVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeLong(obj as? Long ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())
}

private class Float4VectorWriter(override val vector: Float4Vector) : ScalarVectorWriter(vector) {
    override fun writeFloat(v: Float) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeFloat(obj as? Float ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeFloat(v.readFloat())
}

private class Float8VectorWriter(override val vector: Float8Vector) : ScalarVectorWriter(vector) {
    override fun writeDouble(v: Double) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = writeDouble(obj as? Double ?: throw InvalidWriteObjectException(field.fieldType, obj))
    override fun writeValue0(v: ValueReader) = writeDouble(v.readDouble())
}

private class DateDayVectorWriter(override val vector: DateDayVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any) = when (obj) {
        is LocalDate -> writeInt(obj.toEpochDay().toInt())
        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeInt(v.readInt())
}

private class DateMilliVectorWriter(override val vector: DateMilliVector) : ScalarVectorWriter(vector) {
    // `v` in days here
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v * 86_400_000)

    override fun writeObject0(obj: Any) = when (obj) {
        is LocalDate -> writeLong(obj.toEpochDay())
        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())
}

private class TimestampVectorWriter(override val vector: TimeStampVector) : ScalarVectorWriter(vector) {
    private val unit: TimeUnit = (vector.field.type as Timestamp).unit

    private fun Date.toLong(): Long = when (unit) {
        TimeUnit.SECOND -> time / 1000
        TimeUnit.MILLISECOND -> time
        MICROSECOND -> time * 1000
        TimeUnit.NANOSECOND -> time * 1_000_000
    }

    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)

    override fun writeObject0(obj: Any): Unit = when (obj) {
        is Date -> writeLong(obj.toLong())
        is Instant -> writeLong(unit.toLong(obj.epochSecond, obj.nano))
        is ZonedDateTime -> writeObject0(obj.toInstant())
        is OffsetDateTime -> writeObject0(obj.toInstant())
        is LocalDateTime -> writeObject0(obj.toInstant(UTC))
        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())
}

private abstract class TimeVectorWriter(vector: FieldVector) : ScalarVectorWriter(vector) {
    private val multiplier: Long = when ((vector.field.type as ArrowType.Time).unit!!) {
        TimeUnit.SECOND -> 1_000_000_000
        TimeUnit.MILLISECOND -> 1_000_000
        MICROSECOND -> 1_000
        TimeUnit.NANOSECOND -> 1
    }

    override fun writeObject0(obj: Any): Unit = when (obj) {
        is LocalTime -> writeLong(obj.toNanoOfDay() * multiplier)
        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())
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

private val DECIMAL_ERROR_KEY = Keyword.intern("xtdb.error", "decimal-error")

private class DecimalVectorWriter(override val vector: DecimalVector) : ScalarVectorWriter(vector) {
    @Throws(RuntimeException::class)
    override fun writeObject0(obj: Any) {
        if (obj !is BigDecimal) throw InvalidWriteObjectException(field.fieldType, obj)
        try {
            vector.setSafe(wp.getPositionAndIncrement(), obj.setScale(vector.scale))
        } catch (e: Exception) {
            throw RuntimeException(DECIMAL_ERROR_KEY, e.message, emptyMap<Keyword, Any>(), e)
        } catch (e: ArithmeticException) {
            throw RuntimeException(DECIMAL_ERROR_KEY, e.message, emptyMap<Keyword, Any>(), e)
        }
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())
}

private class DurationVectorWriter(override val vector: DurationVector) : ScalarVectorWriter(vector) {
    private val unit: TimeUnit = (vector.field.type as ArrowType.Duration).unit

    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeObject0(obj: Any): Unit = when (obj) {
        is Duration -> writeLong(unit.toLong(obj.seconds, obj.nano))
        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())
}

private operator fun PeriodDuration.component1() = period
private operator fun PeriodDuration.component2() = duration

private class IntervalYearVectorWriter(override val vector: IntervalYearVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)

    override fun writeObject0(obj: Any): Unit = when (obj) {
        is PeriodDuration -> writeInt(obj.period.toTotalMonths().toInt())
        is IntervalYearMonth -> writeObject(PeriodDuration(obj.period, Duration.ZERO))
        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())
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

        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())
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

        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }


    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())
}

private class FixedSizeBinaryVectorWriter(override val vector: FixedSizeBinaryVector) : ScalarVectorWriter(vector) {
    override fun writeBytes(v: ByteBuffer) {
        val pos = v.position()

        val idx = wp.getPositionAndIncrement()
        vector.setIndexDefined(idx)
        vector.dataBuffer.setBytes((idx * vector.byteWidth).toLong(), v)

        v.position(pos)
    }

    override fun writeObject0(obj: Any) = writeBytes(
        when (obj) {
            is ByteBuffer -> obj
            is ByteArray -> ByteBuffer.wrap(obj)

            else -> throw InvalidWriteObjectException(field.fieldType, obj)
        }
    )

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

abstract class VariableWidthVectorWriter(vector: BaseVariableWidthVector) : ScalarVectorWriter(vector) {
    abstract override val vector: BaseVariableWidthVector

    override fun writeBytes(v: ByteBuffer) {
        val bufPos = v.position()
        vector.setSafe(wp.getPositionAndIncrement(), v, v.position(), v.remaining())
        v.position(bufPos)
    }

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

private class VarCharVectorWriter(override val vector: VarCharVector) : VariableWidthVectorWriter(vector) {

    override fun writeObject0(obj: Any) {
        val str = when (obj) {
            is String -> obj
            else -> throw InvalidWriteObjectException(field.fieldType, obj)
        }

        writeBytes(UTF_8.newEncoder().encode(CharBuffer.wrap(str)))
    }
}

private class VarBinaryVectorWriter(override val vector: VarBinaryVector) : VariableWidthVectorWriter(vector) {
    override fun writeObject0(obj: Any) =
        writeBytes(
            when (obj) {
                is ByteBuffer -> obj
                is ByteArray -> ByteBuffer.wrap(obj)
                else -> throw InvalidWriteObjectException(field.fieldType, obj)
            }
        )

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

abstract class ExtensionVectorWriter(
    final override val vector: XtExtensionVector<*>,
    notify: FieldChangeListener? = null,
) :
    ScalarVectorWriter(vector) {
    override val field get() = vector.field

    internal val inner = writerFor(vector.underlyingVector) {
        vector.field = Field(field.name, field.fieldType, it.children)
        notify(field)
    }

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
    override fun writeValue0(v: ValueReader) = inner.writeValue(v)

    override fun structKeyWriter(key: String) = inner.structKeyWriter(key)
    override fun structKeyWriter(key: String, fieldType: FieldType) = inner.structKeyWriter(key, fieldType)
    override fun endStruct() = inner.endStruct()

    override fun listElementWriter() = inner.listElementWriter()
    override fun listElementWriter(fieldType: FieldType) = inner.listElementWriter(fieldType)
    override fun endList() = inner.endList()

    override fun rowCopier(src: ValueVector): RowCopier = when {
        src is NullVector -> nullToVecCopier(this)
        src is DenseUnionVector -> duvToVecCopier(this, src)
        src !is XtExtensionVector<*> || src.javaClass != vector.javaClass || (src.field.isNullable && !field.isNullable) ->
            throw InvalidCopySourceException(src.field.fieldType, field.fieldType)

        else -> inner.rowCopier(src.underlyingVector)
    }
}

internal class KeywordVectorWriter(vector: KeywordVector) : ExtensionVectorWriter(vector, null) {
    override fun writeObject0(obj: Any) =
        if (obj !is Keyword) throw InvalidWriteObjectException(field.fieldType, obj)
        else super.writeObject0(obj.sym.toString())

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

internal class UuidVectorWriter(vector: UuidVector) : ExtensionVectorWriter(vector, null) {
    override fun writeObject0(obj: Any) =
        if (obj !is UUID) throw InvalidWriteObjectException(field.fieldType, obj)
        else
            super.writeObject0(ByteBuffer.allocate(16).also {
                it.putLong(obj.mostSignificantBits)
                it.putLong(obj.leastSignificantBits)
                it.position(0)
            })

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

internal class UriVectorWriter(vector: UriVector) : ExtensionVectorWriter(vector, null) {
    override fun writeObject0(obj: Any) =
        if (obj !is URI) throw InvalidWriteObjectException(field.fieldType, obj)
        else super.writeObject0(obj.toString())

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

internal class TransitVectorWriter(vector: TransitVector) : ExtensionVectorWriter(vector, null) {
    override fun writeObject0(obj: Any) =
        when (obj) {
            is ClojureForm, is RuntimeException, is xtdb.IllegalArgumentException,
            -> super.writeObject0(requiringResolve("xtdb.serde/write-transit")(obj) as ByteArray)

            else -> throw InvalidWriteObjectException(field.fieldType, obj)
        }

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}

internal class TsTzRangeVectorWriter(vector: TsTzRangeVector, notify: FieldChangeListener?) :
    ExtensionVectorWriter(vector, notify) {

    override fun listElementWriter(): IVectorWriter =
        inner.listElementWriter(FieldType.notNullable(Timestamp(MICROSECOND, "UTC")))

    override fun writeObject0(obj: Any) = when (obj) {
        is ZonedDateTimeRange -> {
            listElementWriter(FieldType.notNullable(Timestamp(MICROSECOND, "UTC"))).let {
                it.writeObject(obj.from)
                if (obj.to == null) it.writeLong(Long.MAX_VALUE) else it.writeObject(obj.to)
            }
            endList()
        }

        is ListValueReader -> super.writeObject0(obj)

        else -> throw InvalidWriteObjectException(field.fieldType, obj)
    }
}

internal class SetVectorWriter(vector: SetVector, notify: FieldChangeListener?) :
    ExtensionVectorWriter(vector, notify) {
    override fun writeObject0(obj: Any) =
        when (obj) {
            is ListValueReader -> super.writeObject0(obj)
            is Set<*> -> super.writeObject0(obj.toList())
            else -> throw InvalidWriteObjectException(field.fieldType, obj)
        }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun promoteChildren(field: Field) {
        if (field.type != this.field.type || (field.isNullable && !this.field.isNullable)) throw FieldMismatch(
            this.field.fieldType,
            field.fieldType
        )
        inner.promoteChildren(Field(field.name, inner.field.fieldType, field.children))
    }
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

    override fun visit(vec: BaseVariableWidthViewVector, notify: FieldChangeListener?): IVectorWriter =
        throw UnsupportedOperationException()

    override fun visit(vec: ListVector, notify: FieldChangeListener?) = ListVectorWriter(vec, notify)
    override fun visit(vec: FixedSizeListVector, notify: FieldChangeListener?) = FixedSizeListVectorWriter(vec, notify)

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

    override fun visit(vec: ExtensionTypeVector<*>, notify: FieldChangeListener?): IVectorWriter = when (vec) {
        is KeywordVector -> KeywordVectorWriter(vec)
        is RegClassVector -> RegClassWriter(vec)
        is RegProcVector -> RegProcWriter(vec)
        is UuidVector -> UuidVectorWriter(vec)
        is UriVector -> UriVectorWriter(vec)
        is TransitVector -> TransitVectorWriter(vec)
        is TsTzRangeVector -> TsTzRangeVectorWriter(vec) { notify(vec.field) }
        is SetVector -> SetVectorWriter(vec) { notify(vec.field) }
        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }
}

@JvmOverloads
fun writerFor(vec: ValueVector, notify: FieldChangeListener? = null): IVectorWriter =
    vec.accept(WriterForVectorVisitor, notify)
