@file:JvmName("Types")

package xtdb

import clojure.lang.Keyword
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.PeriodDuration
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
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.types.*
import xtdb.vector.extensions.*
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URI
import java.nio.ByteBuffer
import java.time.*
import java.util.*

internal val String.asKeyword: Keyword get() = Keyword.intern(this)

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
        RegClassType -> "regclass"
        RegProcType -> "regproc"
        SetType -> "set"
        IntervalMDMType -> "interval-month-day-micro"
        TsTzRangeType -> "tstz-range"
        else -> throw UnsupportedOperationException("not supported for $type")
    }
})

internal val TEMPORAL_COL_TYPE = ArrowType.Timestamp(MICROSECOND, "UTC")
private val TS_MICRO_TYPE = ArrowType.Timestamp(MICROSECOND, null)
private val DATE_DAY_TYPE = ArrowType.Date(DAY)
private val DURATION_MICRO_TYPE = ArrowType.Duration(MICROSECOND)
private val TIME_NANO_TYPE = ArrowType.Time(NANOSECOND, 64)

fun valueToArrowType(obj: Any?) = when (obj) {
    null -> MinorType.NULL.type
    is Boolean -> MinorType.BIT.type
    is Byte -> MinorType.TINYINT.type
    is Short -> MinorType.SMALLINT.type
    is Int -> MinorType.INT.type
    is Long -> MinorType.BIGINT.type
    is Float -> MinorType.FLOAT4.type
    is Double -> MinorType.FLOAT8.type

    is BigDecimal -> {
        val precision = when (obj.precision()) {
            // Java Arrow only supports 128 and 256 bit widths
            in 0..32 -> 32
            in 33..64 -> 64
            else -> throw IllegalArgumentException("unsupported precision: ${obj.precision()}")
        }
        ArrowType.Decimal(precision, obj.scale(), precision * 4)
    }

    is ZonedDateTime -> ArrowType.Timestamp(MICROSECOND, obj.zone.toString())
    is OffsetDateTime -> ArrowType.Timestamp(MICROSECOND, obj.offset.toString())
    is Instant -> TEMPORAL_COL_TYPE
    is Date -> TEMPORAL_COL_TYPE
    is LocalDateTime -> TS_MICRO_TYPE
    is LocalDate -> DATE_DAY_TYPE
    is LocalTime -> TIME_NANO_TYPE
    is Duration -> DURATION_MICRO_TYPE

    is CharSequence -> MinorType.VARCHAR.type
    is ByteArray -> MinorType.VARBINARY.type
    is ByteBuffer -> MinorType.VARBINARY.type
    is UUID -> UuidType
    is URI -> UriType
    is RegClass -> RegClassType
    is RegProc -> RegProcType
    is Keyword -> KeywordType
    is ClojureForm -> TransitType
    is IllegalArgumentException -> TransitType
    is RuntimeException -> TransitType

    is List<*> -> MinorType.LIST.type
    is Set<*> -> SetType

    // TODO support for Arrow maps
    is Map<*, *> -> MinorType.STRUCT.type

    is IntervalYearMonth -> MinorType.INTERVALYEAR.type
    is IntervalDayTime -> MinorType.INTERVALDAY.type
    is IntervalMonthDayNano -> MinorType.INTERVALMONTHDAYNANO.type
    is IntervalMonthDayMicro -> IntervalMDMType

    is ZonedDateTimeRange -> TsTzRangeType

    else -> throw UnsupportedOperationException("unknown object type: ${obj.javaClass}")
}

internal fun Any?.toArrowType() = valueToArrowType(this)
internal fun Any?.toFieldType() = FieldType(this == null, toArrowType(), null)
