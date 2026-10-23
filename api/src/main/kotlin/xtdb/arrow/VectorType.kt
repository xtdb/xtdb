@file:JvmName("Types")

package xtdb.arrow

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
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.arrow.extensions.IntervalMDMType
import xtdb.arrow.extensions.KeywordType
import xtdb.arrow.extensions.OidType
import xtdb.arrow.extensions.RegClassType
import xtdb.arrow.extensions.RegProcType
import xtdb.arrow.extensions.SetType
import xtdb.arrow.extensions.TransitType
import xtdb.arrow.extensions.TsTzRangeType
import xtdb.arrow.extensions.UriType
import xtdb.arrow.extensions.UuidType

typealias FieldName = String
typealias VectorTypes = Map<FieldName, VectorType>

fun schema(vararg fields: Field) = Schema(fields.asIterable())

const val LIST_ELS_NAME = $$"$data$"

// the Arrow spec fixes the names of a map's entries struct and its two children
const val MAP_ENTRIES_NAME = "entries"
const val MAP_KEYS_NAME = "key"
const val MAP_VALUES_NAME = "value"

// likewise a run-end encoded vector's two children, which the spec also fixes in this order
const val REE_RUN_ENDS_NAME = "run_ends"
const val REE_VALUES_NAME = "values"

/** The integer type of a run-end encoded column's run-ends — Arrow allows exactly these three. */
enum class RunEndWidth(val arrowType: ArrowType) {
    I16(I16_TYPE), I32(I32_TYPE), I64(I64_TYPE);

    companion object {
        /** @throws Incorrect if [arrowType] isn't one of the three. */
        @JvmStatic
        fun of(arrowType: ArrowType) =
            entries.find { it.arrowType == arrowType }
                ?: throw Incorrect("run-ends must be i16, i32 or i64, got $arrowType")
    }
}

/**
 * How a column's values are laid out, carried by its [VectorType] beside its arrow type —
 * as a `FieldType` carries a dictionary encoding.
 */
sealed interface Encoding {
    /** The physical field of a column called [name], given the field its values take under the name the layout gives them. */
    fun layout(name: FieldName, values: (FieldName) -> Field): Field

    data object Plain : Encoding {
        override fun layout(name: FieldName, values: (FieldName) -> Field) = values(name)
    }

    data class RunEnd(val width: RunEndWidth) : Encoding {
        override fun layout(name: FieldName, values: (FieldName) -> Field) =
            Field(
                name, FieldType.notNullable(RUN_END_ENCODED_TYPE),
                listOf(
                    Field(REE_RUN_ENDS_NAME, FieldType.notNullable(width.arrowType), emptyList()),
                    values(REE_VALUES_NAME)
                )
            )
    }
}

private fun String.encoded(encoding: Encoding) = when (encoding) {
    Encoding.Plain -> this
    is Encoding.RunEnd -> "[:ree ${encoding.width} $this]"
}

// Arrow could lay a null column out as runs, and so could we - but carrying an encoding on these two objects means
// re-judging every `== Null` and `Null ->` across the codebase, which is onerous for a column nothing writes.
private fun <T : VectorType> T.plainOnly(encoding: Encoding): T =
    if (encoding == Encoding.Plain) this else throw Unsupported("$this can't be laid out as $encoding")

sealed class VectorType {
    /** The values' type, whatever their [encoding]. */
    abstract val arrowType: ArrowType
    abstract val nullable: Boolean

    abstract val encoding: Encoding

    /**
     * This type with its values laid out as [encoding].
     * @throws Unsupported on [Null] and [Nothing], for any encoding but [Encoding.Plain].
     */
    abstract fun withEncoding(encoding: Encoding): VectorType

    @JvmOverloads
    fun runEndEncoded(width: RunEndWidth = RunEndWidth.I32) = withEncoding(Encoding.RunEnd(width))

    /** The type of the values themselves: this type with no encoding at any depth — `this`, where it has none. */
    abstract val unencoded: VectorType

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

        abstract override fun withEncoding(encoding: Encoding): Mono
        abstract override val unencoded: Mono

        override val legs: Iterable<Mono> get() = setOf(unencoded)
        override fun toField(name: FieldName) = toField(name, nullable)

        fun toField(name: FieldName, nullable: Boolean): Field =
            encoding.layout(name) { n ->
                Field(n, FieldType(nullable, arrowType, null), children.map { (c, t) -> t.toField(c) })
            }

        override val asLegField get() = unencoded.toField(arrowType.toLeg())
    }

    data object Null : Mono() {
        override val arrowType get() = NULL_TYPE
        override val children: Map<FieldName, VectorType> get() = emptyMap()
        override val nullable get() = true

        override val encoding get() = Encoding.Plain
        override fun withEncoding(encoding: Encoding) = plainOnly(encoding)
        override val unencoded get() = this

        override val asLegField get() = toField(arrowType.toLeg())

        override val legs: Iterable<Mono> get() = listOf(this)
    }

    data class Scalar @JvmOverloads constructor(
        override val arrowType: ArrowType,
        override val encoding: Encoding = Encoding.Plain,
    ) : Mono() {
        init {
            assert(arrowType != NULL_TYPE) { "Use Null type for NULL_TYPE" }
            assert(arrowType !is ArrowType.Union) { "Use Poly type for UNION_TYPE" }
        }

        override val children get() = emptyMap<FieldName, VectorType>()

        override fun withEncoding(encoding: Encoding) = copy(encoding = encoding)
        override val unencoded get() = if (encoding == Encoding.Plain) this else copy(encoding = Encoding.Plain)

        override fun toString() = "<$arrowType>".encoded(encoding)
    }

    data class Listy @JvmOverloads constructor(
        override val arrowType: ArrowType,
        val elType: VectorType,
        override val encoding: Encoding = Encoding.Plain,
    ) : Mono() {
        override val children get() = mapOf(elsName to elType)

        private val elsName get() = if (arrowType is ArrowType.Map) MAP_ENTRIES_NAME else LIST_ELS_NAME

        override fun withEncoding(encoding: Encoding) = copy(encoding = encoding)

        override val unencoded: Listy by lazy {
            val el = elType.unencoded
            if (encoding == Encoding.Plain && el === elType) this else Listy(arrowType, el)
        }

        override fun toString() = "[<$arrowType> $elType]".encoded(encoding)
    }

    data class Struct @JvmOverloads constructor(
        override val children: Map<FieldName, VectorType>,
        override val encoding: Encoding = Encoding.Plain,
    ) : Mono() {
        override val arrowType get() = STRUCT_TYPE

        override fun withEncoding(encoding: Encoding) = copy(encoding = encoding)

        override val unencoded: Struct by lazy {
            val plainChildren = children.mapValues { (_, t) -> t.unencoded }
            if (encoding == Encoding.Plain && plainChildren.all { (n, t) -> t === children[n] }) this
            else Struct(plainChildren)
        }

        override fun toString() =
            children.entries.joinToString(prefix = "{", postfix = "}") { (n, t) -> "$n: $t" }.encoded(encoding)
    }

    /**
     * The lattice bottom: no values seen yet, no constraint. `Nothing ⊔ X = X`.
     *
     * Distinct from [Null]: merging in [Null] sets the nullable flag (`Null ⊔ I64 = Maybe(I64)`),
     * whereas `Nothing` widens cleanly to the first type it meets. It is the schema-level counterpart
     * of `NullVector(nullable = false)` ("undefined", no values), as opposed to `Null`'s `nullable = true`.
     */
    data object Nothing : VectorType() {
        override val arrowType get() = NULL_TYPE
        override val nullable get() = false

        override val encoding get() = Encoding.Plain
        override fun withEncoding(encoding: Encoding) = plainOnly(encoding)
        override val unencoded get() = this

        // the bottom is absorbed by any join, so it contributes no legs and must never be one
        override val legs: Iterable<Mono> get() = emptyList()

        override fun toField(name: FieldName) = Field(name, FieldType(false, NULL_TYPE, null), emptyList())
        override val asLegField get() = error("Nothing is the lattice bottom; it cannot be a union leg")
    }

    data class Maybe(val mono: Mono) : VectorType() {
        override val arrowType get() = mono.arrowType
        override val nullable = true
        val fieldType: FieldType get() = FieldType.nullable(arrowType)

        override val encoding get() = mono.encoding
        override fun withEncoding(encoding: Encoding) = Maybe(mono.withEncoding(encoding))
        override val unencoded get() = mono.unencoded.let { if (it === mono) this else Maybe(it) }

        override val asLegField get() = unencoded.toField(Null.arrowType.toLeg())

        override val legs: Iterable<Mono> get() = listOf(mono.unencoded, Null)

        override fun toField(name: FieldName) =
            encoding.layout(name) { n -> Field(n, fieldType, mono.children.map { (c, t) -> t.toField(c) }) }

        override fun toString() = "[:? ${mono.unencoded}]".encoded(encoding)
    }

    data class Poly @JvmOverloads constructor(
        private val children: Set<Mono>,
        override val encoding: Encoding = Encoding.Plain,
    ) : VectorType() {
        companion object {
            private val fieldType = FieldType.notNullable(UNION_TYPE)
        }

        init {
            require(children.all { it.unencoded === it }) { "a union's legs are plain - the layout is the union's" }
        }

        override fun withEncoding(encoding: Encoding) = copy(encoding = encoding)
        override val unencoded get() = if (encoding == Encoding.Plain) this else copy(encoding = Encoding.Plain)

        override val nullable by lazy { children.any { it == Null } }

        private val singleMono: Mono? by lazy {
            (children - Null).singleOrNull()
        }

        override val arrowType get() = singleMono?.arrowType ?: UNION_TYPE

        override fun toField(name: FieldName): Field =
            encoding.layout(name) { n ->
                singleMono?.toField(n, nullable) ?: Field(n, fieldType, children.map { it.asLegField })
            }

        override val asLegField get() = unencoded.toField(arrowType.toLeg())

        override val legs get() = children
    }

    companion object {

        /**
         * What a source contributes for a column it does not record — the input to the join that produces
         * the column's declared type.
         *
         * A source states only the columns it has values for, but a scan needs an answer for every column
         * it projects, so a source's map is read as a total function: its recorded type where there is one,
         * and this everywhere else. Callers index [recorded] themselves and hold this alongside it, because
         * it is constant for a fixed [recorded] while the lookup is per column.
         *
         * The question is not "do you hold this column" but "what will your rows read for it", so it turns
         * on whether the source has any *put* rows: put rows lacking the column read as null, and no put
         * rows means no cells at all.
         *
         * Derived from [recorded] rather than carried alongside it, because nothing carries a put-row count:
         * every row count we keep includes deletes and erases, which is the distinction being drawn. Nor
         * does put-leg presence answer it — declaring a column materialises the leg without writing a row.
         * What does: `CREATE TABLE` records its declared columns as `Nothing` (`declareColumns` writes
         * `NULL_TYPE, nullable = false`), deletes and erases never materialise a put leg at all, and any
         * value written through a put makes its column non-`Nothing`. So a non-`Nothing` entry means some
         * put wrote a value.
         *
         * That direction is unconditional. The converse holds only while every put leaves at least one
         * non-`Nothing` doc column — true of SQL, XTQL and `writePut`, which all fold `_id` into the doc,
         * but not of a hand-built put with an empty doc struct. #5919 records the count rather than
         * inferring it, which reduces this to a comparison and retires the caveat.
         */
        @JvmStatic
        fun absentContribution(recorded: Map<String, VectorType>): VectorType =
            if (recorded.values.any { it != Nothing }) Null else Nothing

        @JvmStatic
        @JvmOverloads
        fun maybe(type: VectorType, nullable: Boolean = true): VectorType {
            if (!nullable) return type

            return when (type) {
                Null -> type
                Nothing -> Null
                is Mono -> Maybe(type)
                is Maybe -> type
                is Poly -> Poly(type.legs + Null, type.encoding)
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
            val plainLegs = legs.mapTo(mutableSetOf()) { it.unencoded }
            val nullable = Null in plainLegs
            val withoutNull = plainLegs - Null

            return when (withoutNull.size) {
                0 -> Null
                1 -> maybe(withoutNull.single(), nullable)
                else -> Poly(plainLegs)
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

        /**
         * A map's Arrow layout is fixed by its key and value types, independently of which keys are
         * present — so this is what to declare for a dictionary of arbitrary keys, where a [Struct]
         * would have to enumerate them up front.
         */
        @JvmStatic
        @JvmOverloads
        fun mapTypeOf(keyType: VectorType, valType: VectorType, keysSorted: Boolean = false) =
            Listy(ArrowType.Map(keysSorted), structOf(MAP_KEYS_NAME to keyType, MAP_VALUES_NAME to valType))

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
                // a non-nullable NULL field is the lattice bottom (Nothing), not the nullable Null marker -
                // so a declared-but-empty column survives a block flush without polluting to Null/Maybe
                NULL_TYPE -> if (isNullable) Null else Nothing
                LIST_TYPE, SetType, TsTzRangeType, is ArrowType.FixedSizeList, is ArrowType.Map ->
                    Listy(type, children.single().asType)
                STRUCT_TYPE -> Struct(children.associate { it.name to it.asType })
                is ArrowType.Union -> fromLegs(children.map { leg ->
                    leg.asType.also {
                        if (it.unencoded !== it) throw Unsupported("union leg '${leg.name}' can't be run-end encoded")
                    }
                })

                is ArrowType.RunEndEncoded -> {
                    require(children.size == 2) { "run-end encoded fields have exactly two children" }
                    children[1].asType.withEncoding(Encoding.RunEnd(RunEndWidth.of(children[0].type)))
                }

                else -> Scalar(type)
            }.let { maybe(it, isNullable) }
    }
}
