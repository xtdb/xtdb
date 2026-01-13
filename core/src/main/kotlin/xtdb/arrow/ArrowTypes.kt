@file:JvmName("ArrowTypes")

package xtdb.arrow

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.DateUnit.DAY
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.FloatingPointPrecision.*
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.IntervalUnit.*
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.VectorType.Companion.asVectorType
import xtdb.vector.extensions.*

@JvmField
val NULL_TYPE: ArrowType = ArrowType.Null.INSTANCE

@JvmField
val UTF8_TYPE: ArrowType = MinorType.VARCHAR.type

@JvmField
val LIST_TYPE: ArrowType = MinorType.LIST.type

@JvmField
val STRUCT_TYPE: ArrowType = MinorType.STRUCT.type

@JvmField
val UNION_TYPE: ArrowType = MinorType.DENSEUNION.type

private fun DateUnit.toLegPart() = when (this) {
    DAY -> "day"
    DateUnit.MILLISECOND -> "milli"
}

private fun TimeUnit.toLegPart() = when (this) {
    SECOND -> "second"; MILLISECOND -> "milli"; MICROSECOND -> "micro"; NANOSECOND -> "nano"
}

private fun FloatingPointPrecision.toLeg() = when (this) {
    HALF -> "f16"; SINGLE -> "f32"; DOUBLE -> "f64"
}

private fun IntervalUnit.toLegPart() = when (this) {
    YEAR_MONTH -> "year-month"
    DAY_TIME -> "day-time"
    MONTH_DAY_NANO -> "month-day-nano"
}

private val TZ_STR_CACHE = Caffeine.newBuilder().build<String, String> { it.lowercase().replace(Regex("[/:]"), "_") }

fun ArrowType.toLeg(): String = accept(object : ArrowTypeVisitor<String> {
    override fun visit(type: ArrowType.Null) = "null"
    override fun visit(type: ArrowType.Struct) = "struct"
    override fun visit(type: ArrowType.List) = "list"
    override fun visit(type: ArrowType.LargeList) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.FixedSizeList) = "fixed-size-list-${type.listSize}"
    override fun visit(type: ArrowType.ListView) = throw UnsupportedOperationException()
    override fun visit(p0: ArrowType.LargeListView?) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.Union) = "union"
    override fun visit(type: ArrowType.Map) = if (type.keysSorted) "map-sorted" else "map-unsorted"
    override fun visit(type: ArrowType.Int) = "${if (type.isSigned) "i" else "u"}${type.bitWidth}"
    override fun visit(type: ArrowType.FloatingPoint) = type.precision.toLeg()
    override fun visit(type: ArrowType.Utf8) = "utf8"
    override fun visit(type: ArrowType.LargeUtf8) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.Utf8View) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.Binary) = "binary"
    override fun visit(type: ArrowType.LargeBinary) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.FixedSizeBinary) = "fixed-size-binary-${type.byteWidth}"
    override fun visit(type: ArrowType.BinaryView) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.Bool) = "bool"
    override fun visit(type: ArrowType.Decimal) = "decimal-${type.precision}-${type.scale}-${type.bitWidth}"
    override fun visit(type: ArrowType.Date) = "date-${type.unit.toLegPart()}"
    override fun visit(type: ArrowType.Time) = "time-local-${type.unit.toLegPart()}"

    override fun visit(type: ArrowType.Timestamp): String {
        val tz: String? = type.timezone
        return if (tz != null) "timestamp-tz-${type.unit.toLegPart()}-${TZ_STR_CACHE[tz]}" else "timestamp-local-${type.unit.toLegPart()}"
    }

    override fun visit(type: ArrowType.Interval) = "interval-${type.unit.toLegPart()}"
    override fun visit(type: ArrowType.Duration) = "duration-${type.unit.toLegPart()}"

    override fun visit(p0: ArrowType.RunEndEncoded?) = throw UnsupportedOperationException()

    override fun visit(type: ArrowType.ExtensionType) = when (type) {
        KeywordType -> "keyword"
        TransitType -> "transit"
        UuidType -> "uuid"
        UriType -> "uri"
        OidType -> "oid"
        RegClassType -> "regclass"
        RegProcType -> "regproc"
        SetType -> "set"
        IntervalMDMType -> "interval-month-day-micro"
        TsTzRangeType -> "tstz-range"
        else -> throw UnsupportedOperationException("not supported for $type")
    }
})

internal fun Any?.toArrowType() = asVectorType.arrowType

fun Field.withName(name: FieldName) = Field(name, fieldType, children)
