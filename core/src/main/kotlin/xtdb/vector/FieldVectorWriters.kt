@file:JvmName("FieldVectorWriters")

package xtdb.vector

import org.apache.arrow.vector.*
import org.apache.arrow.vector.compare.VectorVisitor
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.vector.extensions.*
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.CharBuffer
import kotlin.text.Charsets.UTF_8
import org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE as NULL_TYPE

private fun nullToVecCopier(dest: IVectorWriter): IRowCopier {
    val wp = dest.writerPosition()
    return IRowCopier { _ -> wp.position.also { dest.writeNull() } }
}

private fun duvToVecCopier(dest: IVectorWriter, src: DenseUnionVector): IRowCopier {
    val copiers = src.map { dest.rowCopier(it) }
    return IRowCopier { srcIdx -> copiers[src.getTypeId(srcIdx).toInt()].copyRow(src.getOffset(srcIdx)) }
}

abstract class ScalarVectorWriter(vector: FieldVector) : IVectorWriter {

    protected val wp = IVectorPosition.build(vector.valueCount)

    override val field: Field get() = vector.field

    override fun writerPosition() = wp

    override fun legWriter(leg: ArrowType): IVectorWriter {
        if ((leg == NULL_TYPE && field.isNullable) || leg == field.type) return this
        throw IllegalArgumentException("arrow-type mismatch: got <${field.type}>, requested <$leg>")
    }

    override fun rowCopier(src: ValueVector): IRowCopier {
        return when (src) {
            is NullVector -> nullToVecCopier(this)
            is DenseUnionVector -> duvToVecCopier(this, src)
            else -> IRowCopier { srcIdx ->
                val pos = wp.getPositionAndIncrement()
                this.vector.copyFromSafe(srcIdx, pos, src)
                pos
            }
        }
    }
}

class NullVectorWriter(override val vector: NullVector) : ScalarVectorWriter(vector) {
    override fun writeValue0(v: IValueReader) = writeNull()
    override fun rowCopier(src: ValueVector) = IRowCopier { _ -> wp.position.also { writeNull() } }
}

private class BitVectorWriter(override val vector: BitVector) : ScalarVectorWriter(vector) {
    override fun writeBoolean(v: Boolean) = vector.setSafe(wp.getPositionAndIncrement(), if (v) 1 else 0)
    override fun writeValue0(v: IValueReader) = writeBoolean(v.readBoolean())
}

private class TinyIntVectorWriter(override val vector: TinyIntVector) : ScalarVectorWriter(vector) {
    override fun writeByte(v: Byte) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeByte(v.readByte())
}

private class SmallIntVectorWriter(override val vector: SmallIntVector) : ScalarVectorWriter(vector) {
    override fun writeShort(v: Short) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeShort(v.readShort())
}

private class IntVectorWriter(override val vector: IntVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeInt(v.readInt())
}

private class BigIntVectorWriter(override val vector: BigIntVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class Float4VectorWriter(override val vector: Float4Vector) : ScalarVectorWriter(vector) {
    override fun writeFloat(v: Float) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeFloat(v.readFloat())
}

private class Float8VectorWriter(override val vector: Float8Vector) : ScalarVectorWriter(vector) {
    override fun writeDouble(v: Double) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeDouble(v.readDouble())
}

private class DateDayVectorWriter(override val vector: DateDayVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeInt(v.readInt())
}

private class DateMilliVectorWriter(override val vector: DateMilliVector) : ScalarVectorWriter(vector) {
    // `v` in days here
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v * 86_400_000)

    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class TimestampVectorWriter(override val vector: TimeStampVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class TimeSecVectorWriter(override val vector: TimeSecVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v.toInt())
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class TimeMilliVectorWriter(override val vector: TimeMilliVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v.toInt())
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class TimeMicroVectorWriter(override val vector: TimeMicroVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class TimeNanoVectorWriter(override val vector: TimeNanoVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private class DecimalVectorWriter(override val vector: DecimalVector) : ScalarVectorWriter(vector) {
    override fun writeObject(v: Any?) {
        require(v is BigDecimal)
        vector.setSafe(wp.getPositionAndIncrement(), v.setScale(vector.scale))
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class DurationVectorWriter(override val vector: DurationVector) : ScalarVectorWriter(vector) {
    override fun writeLong(v: Long) = vector.setSafe(wp.getPositionAndIncrement(), v)
    override fun writeValue0(v: IValueReader) = writeLong(v.readLong())
}

private operator fun PeriodDuration.component1() = period
private operator fun PeriodDuration.component2() = duration

private class IntervalYearVectorWriter(override val vector: IntervalYearVector) : ScalarVectorWriter(vector) {
    override fun writeInt(v: Int) = vector.setSafe(wp.getPositionAndIncrement(), v)

    override fun writeObject(v: Any?) {
        require(v is PeriodDuration)
        writeInt(v.period.toTotalMonths().toInt())
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class IntervalDayVectorWriter(override val vector: IntervalDayVector) : ScalarVectorWriter(vector) {
    override fun writeObject(v: Any?) {
        require(v is PeriodDuration)

        val (p, d) = v
        require(p.years == 0 && p.months == 0)

        vector.setSafe(
            wp.getPositionAndIncrement(),
            p.days,
            Math.addExact(Math.multiplyExact(d.seconds, 1_000).toInt(), d.toMillisPart())
        )
    }

    override fun writeValue0(v: IValueReader) = writeObject(v.readObject())
}

private class IntervalMdnVectorWriter(override val vector: IntervalMonthDayNanoVector) : ScalarVectorWriter(vector) {
    override fun writeObject(v: Any?) {
        require(v is PeriodDuration)
        val (p, d) = v

        vector.setSafe(
            wp.getPositionAndIncrement(),
            p.toTotalMonths().toInt(),
            p.days,
            Math.addExact(Math.multiplyExact(d.seconds, 1_000_000_000), d.toNanosPart().toLong())
        )
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

    override fun writeObject(v: Any?) {
        require(v is ByteArray)
        writeBytes(ByteBuffer.wrap(v))
    }

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

    override fun writeObject(v: Any?) {
        require(v is CharSequence)
        writeBytes(UTF_8.newEncoder().encode(CharBuffer.wrap(v)))
    }
}

private class VarBinaryVectorWriter(override val vector: VarBinaryVector) : VariableWidthVectorWriter(vector) {
    override fun writeObject(v: Any?) {
        require(v is ByteArray)
        writeBytes(ByteBuffer.wrap(v))
    }

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
    override fun writeObject(v: Any?) = inner.writeObject(v)
    override fun writeValue0(v: IValueReader) = inner.writeValue(v)

    override fun rowCopier(src: ValueVector): IRowCopier = when (src) {
        is NullVector -> nullToVecCopier(this)
        is DenseUnionVector -> duvToVecCopier(this, src)
        is XtExtensionVector<*> -> inner.rowCopier(src.underlyingVector)
        else -> throw IllegalArgumentException("unknown src type for rowCopier: ${src.javaClass.simpleName}, dest ${vector.javaClass.simpleName}")
    }
}

object WriterForVectorVisitor : VectorVisitor<IVectorWriter, Nothing?> {
    override fun visit(vec: BaseFixedWidthVector, value: Nothing?) = when (vec) {
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

    override fun visit(vec: BaseVariableWidthVector, value: Nothing?) = when (vec) {
        is VarCharVector -> VarCharVectorWriter(vec)
        is VarBinaryVector -> VarBinaryVectorWriter(vec)

        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }

    override fun visit(vec: BaseLargeVariableWidthVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")
    override fun visit(vec: ListVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")
    override fun visit(vec: FixedSizeListVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")
    override fun visit(vec: LargeListVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")
    override fun visit(vec: NonNullableStructVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")
    override fun visit(vec: UnionVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")
    override fun visit(vec: DenseUnionVector, value: Nothing?): IVectorWriter = TODO("Not yet implemented")

    override fun visit(vec: NullVector, value: Nothing?) = NullVectorWriter(vec)

    override fun visit(vec: ExtensionTypeVector<*>, value: Nothing?) = when (vec) {
        is KeywordVector -> ExtensionVectorWriter(vec, VarCharVectorWriter(vec.underlyingVector))
        is UuidVector -> ExtensionVectorWriter(vec, FixedSizeBinaryVectorWriter(vec.underlyingVector))
        is UriVector -> ExtensionVectorWriter(vec, VarCharVectorWriter(vec.underlyingVector))
        is TransitVector -> ExtensionVectorWriter(vec, VarBinaryVectorWriter(vec.underlyingVector))
        is AbsentVector -> ExtensionVectorWriter(vec, NullVectorWriter(vec.underlyingVector))
        else -> throw UnsupportedOperationException("unknown vector: ${vec.javaClass.simpleName}")
    }
}


fun writerFor(vec: FieldVector): IVectorWriter = vec.accept(WriterForVectorVisitor, null)
