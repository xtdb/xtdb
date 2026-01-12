@file:JvmName("Types")

package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.vector.PeriodDuration
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
import xtdb.types.Oid
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
typealias VectorTypes = Map<FieldName, VectorType>

fun schema(vararg fields: Field) = Schema(fields.asIterable())

internal const val LIST_ELS_NAME = $$"$data$"

data class VectorType(
    val arrowType: ArrowType,
    @get:JvmName("isNullable")
    val nullable: Boolean = false,
    val children: Map<FieldName, VectorType> = emptyMap()
): Iterable<VectorType> {

    val fieldType get() = FieldType(nullable, arrowType, null)

    fun toField(name: FieldName): Field = Field(name, fieldType, children.map { (n, t) -> t.toField(n) })

    val asLegField get() = toField(arrowType.toLeg())

    private val unionLegs get() = if (arrowType is ArrowType.Union) children.values.toList() else listOf(this)

    val firstChildOrNull get() = children.entries.firstOrNull()?.value

    private val splitNull get() = when {
        arrowType is ArrowType.Null -> listOf(this)
        nullable -> listOf(NULL, copy(nullable = false))
        else -> listOf(this)
    }

    override fun iterator(): Iterator<VectorType> = unionLegs.flatMap { it.splitNull }.iterator()

    companion object {

        @JvmStatic
        @JvmOverloads
        fun maybe(type: VectorType, nullable: Boolean = true): VectorType =
            if (nullable && type.arrowType is ArrowType.Union) {
                // For unions, add a null leg instead of setting nullable flag
                val nullLeg = NULL.arrowType.toLeg()
                if (nullLeg in type.children) type
                else type.copy(children = type.children + (nullLeg to NULL))
            } else {
                type.copy(nullable = nullable)
            }

        @JvmStatic
        fun maybe(type: ArrowType, nullable: Boolean = true, vararg children: Pair<FieldName, VectorType>) =
            VectorType(type, nullable, children.toMap())

        fun just(type: ArrowType, vararg children: Pair<FieldName, VectorType>) = VectorType(type, false, children.toMap())

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
        val INSTANT = VectorType(ArrowType.Timestamp(MICROSECOND, "UTC"))

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
        val OID = VectorType(OidType)

        @JvmField
        val REG_CLASS = VectorType(RegClassType)

        @JvmField
        val REG_PROC = VectorType(RegProcType)

        @JvmField
        val TSTZ_RANGE = VectorType(TsTzRangeType, false, mapOf(LIST_ELS_NAME to INSTANT))

        @JvmField
        val TIMESTAMP_MICRO = VectorType(ArrowType.Timestamp(MICROSECOND, null))

        @JvmField
        val DATE_DAY = VectorType(ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY))

        @JvmField
        val TIME_MICRO = VectorType(ArrowType.Time(MICROSECOND, 64))

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

        fun unionOf(vararg legs: Pair<FieldName, VectorType>) = unionOf(legs.toMap())
        fun unionOf(legs: Map<FieldName, VectorType>) = VectorType(ArrowType.Union(UnionMode.Dense, null), children = legs)

        fun structOf(vararg fields: Pair<FieldName, VectorType>) = structOf(fields.toMap())
        fun structOf(fields: Map<FieldName, VectorType>) = VectorType(STRUCT_TYPE, children = fields)

        fun listTypeOf(el: VectorType, elName: FieldName = LIST_ELS_NAME) =
            just(LIST_TYPE, elName to el)

        infix fun FieldName.asListOf(el: VectorType) = this to listTypeOf(el)

        fun fixedSizeList(size: Int, el: VectorType, elName: FieldName = LIST_ELS_NAME) =
            just(ArrowType.FixedSizeList(size), elName to el)

        fun setTypeOf(el: VectorType, nullable: Boolean = false, elName: FieldName = LIST_ELS_NAME) =
            maybe(SetType, nullable, elName to el)

        fun mapTypeOf(
            keyType: VectorType, valueType: VectorType,
            sorted: Boolean = true, entriesName: FieldName = $$"$entries$",
            keyName: FieldName = "key", valueName: FieldName = "value",
        ) =
            just(ArrowType.Map(sorted), entriesName to structOf(keyName to keyType, valueName to valueType))

        @JvmStatic
        fun field(name: FieldName, type: VectorType) = type.toField(name)

        @JvmStatic
        fun field(type: VectorType) = type.asLegField

        infix fun FieldName.ofType(type: VectorType) = type.toField(this)

        fun FieldName.asStructOf(vararg fields: Pair<FieldName, VectorType>) = this to structOf(*fields)
        fun FieldName.asUnionOf(vararg legs: Pair<FieldName, VectorType>) = this to unionOf(*legs)

        @JvmStatic
        @get:JvmName("fromField")
        val Field.asType: VectorType get() = VectorType(type, isNullable, children.associate { it.name to it.asType })

        @JvmStatic
        @get:JvmName("fromValue")
        val Any?.asVectorType: VectorType
            get() = when (this) {
                null -> NULL
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
                    VectorType(ArrowType.Decimal(precision, scale(), precision * 4))
                }

                is ZonedDateTime -> VectorType(ArrowType.Timestamp(MICROSECOND, zone.toString()))
                is OffsetDateTime -> VectorType(ArrowType.Timestamp(MICROSECOND, offset.toString()))
                is Instant -> INSTANT
                is Date -> INSTANT
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
    }
}
