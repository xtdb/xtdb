package xtdb

import clojure.lang.Keyword
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.DateUnit.DAY
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.FloatingPointPrecision.*
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.IntervalUnit.*
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor
import xtdb.vector.extensions.*
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

private fun IntervalUnit.toLegPart() = when(this) {
    YEAR_MONTH -> "year-month"
    DAY_TIME -> "day-time"
    MONTH_DAY_NANO -> "month-day-nano"
}

private val TZ_STR_CACHE = Caffeine.newBuilder().build<String, String> { it.lowercase().replace('/', '_') }

fun ArrowType.toLeg() = accept(object : ArrowTypeVisitor<String> {
    override fun visit(type: ArrowType.Null) = "null"
    override fun visit(type: ArrowType.Struct) = "struct"
    override fun visit(type: ArrowType.List) = "list"
    override fun visit(type: ArrowType.LargeList) = TODO("Not yet implemented")
    override fun visit(type: ArrowType.FixedSizeList) = "fixed-size-list-${type.listSize}"
    override fun visit(type: ArrowType.Union) = "union"
    override fun visit(type: ArrowType.Map) = if (type.keysSorted) "map-sorted" else "map-unsorted"
    override fun visit(type: ArrowType.Int) = "${if (type.isSigned) "i" else "u" }${type.bitWidth}"
    override fun visit(type: ArrowType.FloatingPoint) = type.precision.toLeg()
    override fun visit(type: ArrowType.Utf8) = "utf8"
    override fun visit(type: ArrowType.LargeUtf8) = TODO("Not yet implemented")
    override fun visit(type: ArrowType.Binary) = "binary"
    override fun visit(type: ArrowType.LargeBinary) = TODO("Not yet implemented")
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
        is AbsentType -> "absent"
        is TransitType -> "transit"
        is UuidType -> "uuid"
        is UriType -> "uri"
        is SetType -> "set"
        else -> throw UnsupportedOperationException("not supported for $type")
    }
}).asKeyword
