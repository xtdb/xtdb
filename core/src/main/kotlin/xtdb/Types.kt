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
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.types.ClojureForm
import xtdb.types.IntervalDayTime
import xtdb.types.IntervalMonthDayNano
import xtdb.types.IntervalYearMonth
import xtdb.vector.extensions.*
import java.math.BigDecimal
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

private val TZ_STR_CACHE = Caffeine.newBuilder().build<String, String> { it.lowercase().replace('/', '_') }

fun ArrowType.toLeg() = accept(object : ArrowTypeVisitor<String> {
    override fun visit(type: ArrowType.Null) = "null"
    override fun visit(type: ArrowType.Struct) = "struct"
    override fun visit(type: ArrowType.List) = "list"
    override fun visit(type: ArrowType.LargeList) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.FixedSizeList) = "fixed-size-list-${type.listSize}"
    override fun visit(type: ArrowType.Union) = "union"
    override fun visit(type: ArrowType.Map) = if (type.keysSorted) "map-sorted" else "map-unsorted"
    override fun visit(type: ArrowType.Int) = "${if (type.isSigned) "i" else "u"}${type.bitWidth}"
    override fun visit(type: ArrowType.FloatingPoint) = type.precision.toLeg()
    override fun visit(type: ArrowType.Utf8) = "utf8"
    override fun visit(type: ArrowType.LargeUtf8) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.Binary) = "binary"
    override fun visit(type: ArrowType.LargeBinary) = throw UnsupportedOperationException()
    override fun visit(type: ArrowType.FixedSizeBinary) = "fixed-size-binary-${type.byteWidth}"
    override fun visit(type: ArrowType.Bool) = "bool"
    override fun visit(type: ArrowType.Decimal) = "decimal"
    override fun visit(type: ArrowType.Date) = "date-${type.unit.toLegPart()}"
    override fun visit(type: ArrowType.Time) = "time-local-${type.unit.toLegPart()}"

    override fun visit(type: ArrowType.Timestamp): String {
        val tz: String? = type.timezone
        return if (tz != null) "timestamp-tz-${type.unit.toLegPart()}-${TZ_STR_CACHE[tz]}" else "timestamp-local-${type.unit.toLegPart()}"
    }

    override fun visit(type: ArrowType.Interval) = "interval-${type.unit.toLegPart()}"
    override fun visit(type: ArrowType.Duration) = "duration-${type.unit.toLegPart()}"

    override fun visit(type: ArrowType.ExtensionType) = when (type) {
        is KeywordType -> "keyword"
        is TransitType -> "transit"
        is UuidType -> "uuid"
        is UriType -> "uri"
        is SetType -> "set"
        else -> throw UnsupportedOperationException("not supported for $type")
    }
}).asKeyword

private val TS_TZ_MICRO_TYPE = ArrowType.Timestamp(MICROSECOND, "UTC")
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

    // HACK we should support parameterised decimals here
    is BigDecimal -> ArrowType.Decimal(38, 19, 128)

    is ZonedDateTime -> ArrowType.Timestamp(MICROSECOND, obj.zone.toString())
    is OffsetDateTime -> ArrowType.Timestamp(MICROSECOND, obj.offset.toString())
    is Instant -> TS_TZ_MICRO_TYPE
    is Date -> TS_TZ_MICRO_TYPE
    is LocalDateTime -> TS_MICRO_TYPE
    is LocalDate -> DATE_DAY_TYPE
    is LocalTime -> TIME_NANO_TYPE
    is Duration -> DURATION_MICRO_TYPE

    is CharSequence -> MinorType.VARCHAR.type
    is ByteArray -> MinorType.VARBINARY.type
    is ByteBuffer -> MinorType.VARBINARY.type
    is UUID -> UuidType
    is URI -> UriType
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
    is IntervalMonthDayNano, is PeriodDuration -> MinorType.INTERVALMONTHDAYNANO.type

    else -> throw UnsupportedOperationException("unknown object type: ${obj.javaClass}")
}

internal fun Any?.toArrowType() = valueToArrowType(this)
internal fun Any?.toFieldType() = FieldType(this == null, toArrowType(), null)

val NULL_FIELD = Field("\$data\$", FieldType.nullable(MinorType.NULL.type), null)

fun toNotNullable(field: Field) = Field(field.name, FieldType.notNullable(field.type), field.children)

fun isSubType(subField: Field, field: Field): Boolean {
    // order is important here
    when {
        subField.type !is ArrowType.Union && field.type is ArrowType.Union ->{
            val childFields = field.children.associateByTo(LinkedHashMap()) { it.type }
            return when (val childField = childFields[subField.type]) {
                null -> false
                else -> isSubType(subField, childField) || (isSubType(toNotNullable(subField), childField) && childFields[MinorType.NULL.type] != null)
            }
        }
        subField.isNullable && !field.isNullable -> return false
        // Unions might have different typeIds
        subField.type != field.type && subField.type !is ArrowType.Union && field.type !is ArrowType.Union -> return false
        subField.type is ArrowType.List -> isSubType(subField.children[0], field.children[0])
        subField.type is ArrowType.Struct -> {
            val childFields = field.children.associateByTo(LinkedHashMap()) { it.name }
            for (subFieldChild in subField.children) {
                when (val childField = childFields[subFieldChild.name]) {
                    null -> return false
                    else -> if (!isSubType(subFieldChild, childField)) return false
                }
            }
            return true
        }
        subField.type is ArrowType.Union -> {
            val childFields = field.children.associateByTo(LinkedHashMap()) { it.type }
            for (subFieldChild in subField.children) {
                when (val fieldChild = childFields[subFieldChild.type]) {
                    null -> return false
                    else -> if (!isSubType(subFieldChild, fieldChild)) return false
                }
            }
            return true
        }
    }
    return true
}

fun Field.withName(name: String) = Field(name, fieldType, children)