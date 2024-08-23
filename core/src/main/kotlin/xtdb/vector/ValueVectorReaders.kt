package xtdb.vector

import org.apache.arrow.vector.*
import org.apache.arrow.vector.compare.VectorVisitor
import org.apache.arrow.vector.complex.*
import xtdb.vector.ValueVectorReader.*
import xtdb.vector.extensions.*

object VecToReader : VectorVisitor<IVectorReader, Any?> {
    override fun visit(v: BaseFixedWidthVector, value: Any?): IVectorReader = when (v) {
        is BitVector -> bitVector(v)
        is TinyIntVector -> tinyIntVector(v)
        is SmallIntVector -> smallIntVector(v)
        is IntVector -> intVector(v)
        is BigIntVector -> bigIntVector(v)
        is Float4Vector -> float4Vector(v)
        is Float8Vector -> float8Vector(v)

        is DateDayVector -> dateDayVector(v)
        is DateMilliVector -> dateMilliVector(v)

        is TimeStampSecVector -> timestampVector(v)
        is TimeStampMilliVector -> timestampVector(v)
        is TimeStampMicroVector -> timestampVector(v)
        is TimeStampNanoVector -> timestampVector(v)

        is TimeStampSecTZVector -> timestampSecTzVector(v)
        is TimeStampMilliTZVector -> timestampMilliTzVector(v)
        is TimeStampMicroTZVector -> timestampMicroTzVector(v)
        is TimeStampNanoTZVector -> timestampNanoTzVector(v)

        is TimeSecVector -> timeSecVector(v)
        is TimeMilliVector -> timeMilliVector(v)
        is TimeMicroVector -> timeMicroVector(v)
        is TimeNanoVector -> timeNanoVector(v)

        is DurationVector -> durationVector(v)

        is IntervalYearVector -> intervalYearVector(v)
        is IntervalDayVector -> intervalDayVector(v)
        is IntervalMonthDayNanoVector -> intervalMdnVector(v)

        is FixedSizeBinaryVector -> fixedSizeBinaryVector(v)
        else -> ValueVectorReader(v)
    }

    override fun visit(v: BaseVariableWidthVector, value: Any?): IVectorReader = when (v) {
        is VarCharVector -> varCharVector(v)
        is VarBinaryVector -> varBinaryVector(v)
        else -> ValueVectorReader(v)
    }

    override fun visit(v: BaseLargeVariableWidthVector, value: Any?) = TODO("Not yet implemented")
    override fun visit(v: BaseVariableWidthViewVector, value: Any?) = TODO("Not yet implemented")

    override fun visit(v: ListVector, value: Any?): IVectorReader = when (v) {
        is MapVector -> mapVector(v)
        else -> listVector(v)
    }

    override fun visit(v: FixedSizeListVector, value: Any?): IVectorReader = fixedSizeListVector(v)
    override fun visit(v: LargeListVector, value: Any?): IVectorReader = throw UnsupportedOperationException()
    override fun visit(v: NonNullableStructVector, value: Any?): IVectorReader = structVector(v)
    override fun visit(v: UnionVector, value: Any?): IVectorReader = throw UnsupportedOperationException()
    override fun visit(v: DenseUnionVector, value: Any?): IVectorReader = denseUnionVector(v)
    override fun visit(v: NullVector, value: Any?): IVectorReader = ValueVectorReader(v)

    override fun visit(v: ExtensionTypeVector<*>, value: Any?): IVectorReader = when (v) {
        is KeywordVector -> keywordVector(v)
        is RegClassVector -> regClassVector(v)
        is UuidVector -> uuidVector(v)
        is UriVector -> uriVector(v)
        is TransitVector -> transitVector(v)
        is TsTzRangeVector -> tstzRangeVector(v)
        is SetVector -> setVector(v)
        else -> ValueVectorReader(v)
    }
}

fun from(v: ValueVector) = v.accept(VecToReader, null)
