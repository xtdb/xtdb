@file:JvmName("Types")

package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.vector.PeriodDuration
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
import org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
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
typealias VectorTypes = Map<FieldName, VectorType>

fun schema(vararg fields: Field) = Schema(fields.asIterable())

const val LIST_ELS_NAME = $$"$data$"

sealed class VectorType {
    abstract val arrowType: ArrowType
    abstract val nullable: Boolean

    abstract fun toField(name: FieldName): Field

    abstract val asLegField: Field

    abstract val legs: Iterable<Mono>

    @get:JvmName("asMono")
    val asMono get() = this as Mono

    sealed class Mono() : VectorType() {
        abstract val children: Map<FieldName, VectorType>

        override val nullable: Boolean get() = arrowType == NULL_TYPE

        val fieldType get() = FieldType(nullable, arrowType, null)
        val firstChildOrNull get() = children.entries.firstOrNull()?.value

        override val legs: Iterable<Mono> get() = setOf(this)
        override fun toField(name: FieldName) = toField(name, nullable)

        fun toField(name: FieldName, nullable: Boolean): Field =
            Field(name, FieldType(nullable, arrowType, null), children.map { (n, t) -> t.toField(n) })

        override val asLegField get() = toField(arrowType.toLeg())
    }

    data object Null : Mono() {
        override val arrowType get() = NULL_TYPE
        override val children: Map<FieldName, VectorType> get() = emptyMap()
        override val nullable get() = true

        override val asLegField get() = toField(arrowType.toLeg())

        override val legs: Iterable<Mono> get() = listOf(this)
    }

    data class Scalar(override val arrowType: ArrowType) : Mono() {
        init {
            assert(arrowType != NULL_TYPE) { "Use Null type for NULL_TYPE" }
            assert(arrowType !is ArrowType.Union) { "Use Poly type for UNION_TYPE" }
        }

        override val children get() = emptyMap<FieldName, VectorType>()

        override fun toString() = "<$arrowType>"
    }

    data class Listy(override val arrowType: ArrowType, val elType: VectorType) : Mono() {
        override val children get() = mapOf(LIST_ELS_NAME to elType)

        override fun toString() = "[<$arrowType> $elType]"
    }

    data class Struct(override val children: Map<FieldName, VectorType>) : Mono() {
        override val arrowType get() = STRUCT_TYPE

        override fun toString() = children.entries.joinToString(prefix = "{", postfix = "}") { (n, t) -> "$n: $t" }
    }

    data class Maybe(val mono: Mono) : VectorType() {
        override val arrowType get() = mono.arrowType
        override val nullable = true
        val fieldType: FieldType get() = FieldType.nullable(arrowType)

        override val asLegField get() = toField(Null.arrowType.toLeg())

        override val legs: Iterable<Mono> get() = listOf(mono, Null)

        override fun toField(name: FieldName) =
            Field(name, fieldType, mono.children.map { (n, t) -> t.toField(n) })

        override fun toString() = "[:? $mono]"
    }

    data class Poly(
        private val children: Set<Mono>
    ) : VectorType() {
        companion object {
            private val fieldType = FieldType.notNullable(UNION_TYPE)
        }

        override val nullable by lazy { children.any { it == Null } }

        private val singleMono: Mono? by lazy {
            (children - Null).singleOrNull()
        }

        override val arrowType get() = singleMono?.arrowType ?: UNION_TYPE

        override fun toField(name: FieldName): Field =
            singleMono?.toField(name, nullable)
                ?: Field(name, fieldType, children.map { it.asLegField })

        override val asLegField get() = toField(arrowType.toLeg())

        override val legs get() = children
    }

    companion object {

        @JvmStatic
        @JvmOverloads
        fun maybe(type: VectorType, nullable: Boolean = true): VectorType {
            if (!nullable) return type

            return when (type) {
                Null -> type
                is Mono -> Maybe(type)
                is Maybe -> type
                is Poly -> Poly(type.legs + Null)
            }
        }

        @JvmField
        val BOOL = Scalar(MinorType.BIT.type)

        @JvmField
        val I8 = Scalar(MinorType.TINYINT.type)

        @JvmField
        val I16 = Scalar(MinorType.SMALLINT.type)

        @JvmField
        val I32 = Scalar(MinorType.INT.type)

        @JvmField
        val I64 = Scalar(MinorType.BIGINT.type)

        @JvmField
        val F32 = Scalar(ArrowType.FloatingPoint(SINGLE))

        @JvmField
        val F64 = Scalar(ArrowType.FloatingPoint(DOUBLE))

        @JvmField
        val UTF8 = Scalar(MinorType.VARCHAR.type)

        @JvmField
        val INSTANT = Scalar(ArrowType.Timestamp(MICROSECOND, "UTC"))

        @JvmField
        val IID = Scalar(ArrowType.FixedSizeBinary(16))

        @JvmField
        val VAR_BINARY = Scalar(MinorType.VARBINARY.type)

        @JvmField
        val UUID = Scalar(UuidType)

        @JvmField
        val URI = Scalar(UriType)

        @JvmField
        val KEYWORD = Scalar(KeywordType)

        @JvmField
        val TRANSIT = Scalar(TransitType)

        @JvmField
        val OID = Scalar(OidType)

        @JvmField
        val REG_CLASS = Scalar(RegClassType)

        @JvmField
        val REG_PROC = Scalar(RegProcType)

        @JvmField
        val TSTZ_RANGE = Listy(TsTzRangeType, INSTANT)

        @JvmField
        val TIMESTAMP_MICRO = Scalar(ArrowType.Timestamp(MICROSECOND, null))

        @JvmField
        val DATE_DAY = Scalar(ArrowType.Date(DateUnit.DAY))

        @JvmField
        val TIME_MICRO = Scalar(ArrowType.Time(MICROSECOND, 64))

        @JvmField
        val TIME_NANO = Scalar(ArrowType.Time(TimeUnit.NANOSECOND, 64))

        @JvmField
        val DURATION_MICRO = Scalar(ArrowType.Duration(MICROSECOND))

        @JvmField
        val INTERVAL_YEAR = Scalar(ArrowType.Interval(IntervalUnit.YEAR_MONTH))

        @JvmField
        val INTERVAL_MDN = Scalar(ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO))

        @JvmField
        val INTERVAL_MDM = Scalar(IntervalMDMType)

        @JvmStatic
        fun scalar(arrowType: ArrowType) = Scalar(arrowType)

        fun fromLegs(vararg legs: Mono) = fromLegs(legs.toSet())
        fun fromLegs(legs: Iterable<VectorType>) = fromLegs(legs.flatMapTo(mutableSetOf()) { it.legs })

        @JvmStatic
        fun fromLegs(legs: Set<Mono>): VectorType {
            val nullable = Null in legs
            val withoutNull = legs - Null

            return when (withoutNull.size) {
                0 -> Null
                1 -> maybe(withoutNull.single(), nullable)
                else -> Poly(legs)
            }
        }

        fun FieldName.asUnionFieldOf(legs: Iterable<Pair<FieldName, VectorType>>) =
            Field(this, FieldType.notNullable(UNION_TYPE), legs.map { (n, t) -> t.toField(n) })

        fun FieldName.asUnionFieldOf(vararg legs: Pair<FieldName, VectorType>) = asUnionFieldOf(legs.asIterable())

        fun structOf(vararg fields: Pair<FieldName, VectorType>) = structOf(fields.toMap())

        @JvmStatic
        fun structOf(fields: Map<FieldName, VectorType>) = Struct(fields)

        @JvmStatic
        fun listy(arrowType: ArrowType, el: VectorType) = Listy(arrowType, el)

        fun listTypeOf(el: VectorType) = Listy(LIST_TYPE, el)
        infix fun FieldName.asListOf(el: VectorType) = this to listTypeOf(el)

        fun setTypeOf(el: VectorType) = Listy(SetType, el)

        @JvmStatic
        fun field(name: FieldName, type: VectorType) = type.toField(name)

        @JvmStatic
        fun field(type: VectorType) = type.asLegField

        infix fun FieldName.ofType(type: VectorType) = type.toField(this)

        fun FieldName.asStructOf(vararg fields: Pair<FieldName, VectorType>) = this to structOf(*fields)
        fun FieldName.asUnionOf(vararg legs: Mono) = this to fromLegs(*legs)

        @JvmStatic
        @get:JvmName("fromField")
        val Field.asType: VectorType
            get() = when (type) {
                NULL_TYPE -> Null
                LIST_TYPE, SetType, TsTzRangeType, is ArrowType.FixedSizeList -> Listy(type, children.single().asType)
                STRUCT_TYPE -> Struct(children.associate { it.name to it.asType })
                is ArrowType.Union -> fromLegs(children.map { it.asType })
                else -> Scalar(type)
            }.let { maybe(it, isNullable) }
    }
}
