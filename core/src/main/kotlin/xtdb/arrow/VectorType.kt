@file:JvmName("Types")

package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
import org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.error.Unsupported
import xtdb.time.Interval
import xtdb.time.MICRO_HZ
import xtdb.time.NANO_HZ
import xtdb.types.ClojureForm
import xtdb.types.RegClass
import xtdb.types.RegProc
import xtdb.types.ZonedDateTimeRange
import xtdb.util.normalForm
import xtdb.vector.extensions.*
import java.math.BigDecimal
import java.net.URI
import java.nio.ByteBuffer
import java.time.*
import java.util.*

typealias FieldName = String

fun schema(vararg fields: Field) = Schema(fields.asIterable())

internal const val LIST_ELS_NAME = $$"$data$"

data class VectorType(
    val arrowType: ArrowType,
    @get:JvmName("isNullable")
    val nullable: Boolean = false,
    val children: List<Field> = emptyList()
) {

    val fieldType get() = FieldType(nullable, arrowType, null)

    val asLegField get() = Field(arrowType.toLeg(), fieldType, children)

    companion object {

        fun maybe(type: VectorType, nullable: Boolean = true) = type.copy(nullable = nullable)
        fun maybe(type: ArrowType, nullable: Boolean = true, children: List<Field> = emptyList()) =
            VectorType(type, nullable, children)

        fun maybe(type: ArrowType, vararg children: Field) = maybe(type, children = children.toList())
        fun just(type: ArrowType, children: List<Field> = emptyList()) = VectorType(type, false, children)
        fun just(type: ArrowType, vararg children: Field) = just(type, children.toList())

        @JvmField
        val NULL = VectorType(MinorType.NULL.type, true)

        @JvmField
        val BOOL = VectorType(MinorType.BIT.type)

        @JvmField
        val I8 = VectorType(MinorType.TINYINT.type)

        @JvmField
        val I16 = VectorType(MinorType.SMALLINT.type)

        @JvmField
        val I32 = VectorType(MinorType.INT.type)

        @JvmField
        val I64 = VectorType(MinorType.BIGINT.type)

        @JvmField
        val F32 = VectorType(ArrowType.FloatingPoint(SINGLE))

        @JvmField
        val F64 = VectorType(ArrowType.FloatingPoint(DOUBLE))

        @JvmField
        val UTF8 = VectorType(MinorType.VARCHAR.type)

        @JvmField
        val TEMPORAL = VectorType(ArrowType.Timestamp(MICROSECOND, "UTC"))

        @JvmField
        val IID = VectorType(ArrowType.FixedSizeBinary(16))

        @JvmField
        val VAR_BINARY = VectorType(MinorType.VARBINARY.type)

        @JvmField
        val UUID = VectorType(UuidType)

        @JvmField
        val URI = VectorType(UriType)

        @JvmField
        val KEYWORD = VectorType(KeywordType)

        @JvmField
        val TRANSIT = VectorType(TransitType)

        @JvmField
        val REG_CLASS = VectorType(RegClassType)

        @JvmField
        val REG_PROC = VectorType(RegProcType)

        @JvmField
        val TSTZ_RANGE = VectorType(TsTzRangeType)

        @JvmField
        val TIMESTAMP_MICRO = VectorType(ArrowType.Timestamp(MICROSECOND, null))

        @JvmField
        val DATE_DAY = VectorType(ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY))

        @JvmField
        val TIME_NANO = VectorType(ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 64))

        @JvmField
        val DURATION_MICRO = VectorType(ArrowType.Duration(MICROSECOND))

        @JvmField
        val INTERVAL_YEAR = VectorType(ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.YEAR_MONTH))

        @JvmField
        val INTERVAL_MDN = VectorType(ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.MONTH_DAY_NANO))

        @JvmField
        val INTERVAL_MDM = VectorType(IntervalMDMType)

        @JvmField
        val LIST_TYPE: ArrowType = MinorType.LIST.type

        @JvmField
        val STRUCT_TYPE: ArrowType = MinorType.STRUCT.type

        @JvmField
        val UNION_TYPE: ArrowType = MinorType.DENSEUNION.type

        fun unionOf(vararg legs: Field) = unionOf(legs.toList())
        fun unionOf(legs: List<Field>) = VectorType(ArrowType.Union(UnionMode.Dense, null), children = legs)
        fun FieldName.asUnionOf(vararg legs: Field) = asUnionOf(legs.toList())
        infix fun FieldName.asUnionOf(legs: List<Field>) = ofType(unionOf(legs))

        fun structOf(vararg fields: Field) = structOf(fields.toList())
        fun structOf(fields: List<Field>) = VectorType(STRUCT_TYPE, children = fields)
        fun FieldName.asStructOf(vararg fields: Field) = asStructOf(fields.toList())
        infix fun FieldName.asStructOf(fields: List<Field>) = ofType(structOf(fields))

        fun listTypeOf(el: VectorType, elName: FieldName = LIST_ELS_NAME) =
            just(LIST_TYPE, elName ofType el)

        infix fun FieldName.asListOf(el: VectorType) = this ofType listTypeOf(el)

        fun fixedSizeList(size: Int, el: VectorType, elName: FieldName = LIST_ELS_NAME) =
            just(ArrowType.FixedSizeList(size), elName ofType el)

        fun setTypeOf(el: VectorType, nullable: Boolean = false, elName: FieldName = LIST_ELS_NAME) =
            maybe(SetType, nullable, listOf(elName ofType el))

        fun mapTypeOf(
            key: Field, value: Field,
            sorted: Boolean = true, entriesName: FieldName = $$"$entries$",
        ) =
            just(ArrowType.Map(sorted), entriesName.asStructOf(key, value))

        @JvmStatic
        @JvmName("field")
        infix fun FieldName.ofType(type: VectorType) = Field(this, type.fieldType, type.children)

        @JvmStatic
        @get:JvmName("fromField")
        val Field.asType get() = VectorType(type, isNullable, children)

        @JvmStatic
        @get:JvmName("fromValue")
        val Any?.asVectorType: VectorType
            get() = when (this) {
                null -> NULL
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
                    VectorType(ArrowType.Decimal(precision, scale(), precision * 4))
                }

                is ZonedDateTime -> VectorType(ArrowType.Timestamp(MICROSECOND, zone.toString()))
                is OffsetDateTime -> VectorType(ArrowType.Timestamp(MICROSECOND, offset.toString()))
                is Instant -> TEMPORAL
                is Date -> TEMPORAL
                is LocalDateTime -> TIMESTAMP_MICRO
                is LocalDate -> DATE_DAY
                is LocalTime -> TIME_NANO
                is Duration -> DURATION_MICRO

                is CharSequence -> UTF8
                is ByteArray -> VAR_BINARY
                is ByteBuffer -> VAR_BINARY
                is UUID -> UUID
                is URI -> URI
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
                        structOf(map { (k, v) ->
                            val normalK = when (k) {
                                is String -> k
                                is Keyword -> normalForm(k).sym.toString()
                                else -> error("k is of type ${k?.javaClass}")
                            }
                            normalK ofType v.asVectorType
                        })
                    } else {
                        throw UnsupportedOperationException("Type Maps currently not supported")
                    }

                is Interval -> when {
                    nanos % (NANO_HZ / MICRO_HZ) != 0L -> INTERVAL_MDN
                    nanos != 0L || days != 0 -> INTERVAL_MDM
                    else -> INTERVAL_YEAR
                }

                is ZonedDateTimeRange -> TSTZ_RANGE

                else -> throw UnsupportedOperationException("unknown object type: $javaClass")
            }
    }
}