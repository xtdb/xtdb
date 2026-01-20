@file:JvmName("ValueTypes")

package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.vector.PeriodDuration
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorType.*
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.arrow.VectorType.Companion.DATE_DAY
import xtdb.arrow.VectorType.Companion.DURATION_MICRO
import xtdb.arrow.VectorType.Companion.F32
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I16
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.I8
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.INTERVAL_MDM
import xtdb.arrow.VectorType.Companion.INTERVAL_MDN
import xtdb.arrow.VectorType.Companion.INTERVAL_YEAR
import xtdb.arrow.VectorType.Companion.KEYWORD
import xtdb.arrow.VectorType.Companion.REG_CLASS
import xtdb.arrow.VectorType.Companion.REG_PROC
import xtdb.arrow.VectorType.Companion.TIMESTAMP_MICRO
import xtdb.arrow.VectorType.Companion.TIME_NANO
import xtdb.arrow.VectorType.Companion.TRANSIT
import xtdb.arrow.VectorType.Companion.TSTZ_RANGE
import xtdb.arrow.VectorType.Companion.URI
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.UUID
import xtdb.arrow.VectorType.Companion.VAR_BINARY
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.error.Unsupported
import xtdb.time.Interval
import xtdb.time.MICRO_HZ
import xtdb.time.NANO_HZ
import xtdb.types.ClojureForm
import xtdb.types.RegClass
import xtdb.types.RegProc
import xtdb.types.ZonedDateTimeRange
import xtdb.util.normalForm
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.*
import java.util.*
import java.net.URI as JavaURI
import java.util.UUID as JavaUUID

@get:JvmName("fromValue")
val Any?.asVectorType: Mono
    get() = when (this) {
        null -> Null
        is ValueReader -> readObject().asVectorType
        is ListValueReader -> listTypeOf(mergeTypes((0 until size()).map { nth(it).readObject().asVectorType }))
        is Boolean -> BOOL
        is Byte -> I8
        is Short -> I16
        is Int -> I32
        is Long -> I64
        is Float -> F32
        is Double -> F64

        is BigDecimal -> {
            val precision = when (precision()) {
                // Java Type only supports 128 and 256 bit widths
                in 0..32 -> 32
                in 33..64 -> 64
                else -> throw Unsupported("Unsupported precision: ${precision()}")
            }
            Scalar(ArrowType.Decimal(precision, scale(), precision * 4))
        }

        is ZonedDateTime -> Scalar(ArrowType.Timestamp(MICROSECOND, zone.toString()))
        is OffsetDateTime -> Scalar(ArrowType.Timestamp(MICROSECOND, offset.toString()))
        is Instant -> INSTANT
        is Date -> INSTANT
        is LocalDateTime -> TIMESTAMP_MICRO
        is LocalDate -> DATE_DAY
        is LocalTime -> TIME_NANO
        is java.time.Duration -> DURATION_MICRO

        is CharSequence -> UTF8
        is ByteArray -> VAR_BINARY
        is ByteBuffer -> VAR_BINARY
        is JavaUUID -> UUID
        is JavaURI -> URI
        is RegClass -> REG_CLASS
        is RegProc -> REG_PROC
        is Keyword -> KEYWORD
        is ClojureForm -> TRANSIT
        is IllegalArgumentException -> TRANSIT
        is RuntimeException -> TRANSIT

        is List<*> -> listTypeOf(mergeTypes(map<Any?, VectorType> { it.asVectorType }))

        is Set<*> -> setTypeOf(mergeTypes(map<Any?, VectorType> { it.asVectorType }))

        // TODO support for Type maps
        is Map<*, *> ->
            if (keys.all { it is String || it is Keyword }) {
                structOf(entries.associate { (k, v) ->
                    val normalK = when (k) {
                        is String -> k
                        is Keyword -> normalForm(k).sym.toString()
                        else -> error("k is of type ${k?.javaClass}")
                    }
                    normalK to v.asVectorType
                })
            } else {
                throw UnsupportedOperationException("Type Maps currently not supported")
            }

        is Interval -> when {
            nanos % (NANO_HZ / MICRO_HZ) != 0L -> INTERVAL_MDN
            nanos != 0L || days != 0 -> INTERVAL_MDM
            else -> INTERVAL_YEAR
        }

        is PeriodDuration -> when {
            period.days == 0 && duration.equals(Duration.ZERO) -> INTERVAL_YEAR
            period.toTotalMonths() == 0L && duration.toNanos() % 1_000_000 == 0L -> INTERVAL_MDM
            else -> INTERVAL_MDN
        }

        is ZonedDateTimeRange -> TSTZ_RANGE

        else -> throw UnsupportedOperationException("unknown object type: $javaClass")
    }

internal fun Any?.toArrowType() = asVectorType.arrowType
