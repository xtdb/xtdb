package xtdb.table

import com.google.protobuf.ByteString
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.error.Unsupported
import xtdb.arrow.FieldName
import xtdb.arrow.LIST_ELS_NAME
import xtdb.arrow.MAP_ENTRIES_NAME
import xtdb.arrow.VectorType
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
import xtdb.arrow.toLeg
import xtdb.cols.proto.Decimal
import xtdb.cols.proto.FixedSizeList
import xtdb.cols.proto.Listy
import xtdb.cols.proto.MapType
import xtdb.cols.proto.Maybe
import xtdb.cols.proto.Poly
import xtdb.cols.proto.Struct
import xtdb.cols.proto.VecType
import xtdb.cols.proto.VecType.Simple
import xtdb.cols.proto.VecType.TypeCase
import xtdb.cols.proto.ColumnMeta as ColumnMetaProto
import xtdb.cols.proto.Date as DateProto
import xtdb.cols.proto.Interval as IntervalProto
import xtdb.cols.proto.Time as TimeProto
import xtdb.cols.proto.TimeUnit as TimeUnitProto
import xtdb.cols.proto.Timestamp as TimestampProto
import xtdb.util.HLL
import xtdb.util.toHLL

/** The name a listy type's element is stored under, which its arrow type fixes. */
private fun elsName(arrowType: ArrowType) =
    if (arrowType is ArrowType.Map) MAP_ENTRIES_NAME else LIST_ELS_NAME

private fun TimeUnit.toProto() = when (this) {
    TimeUnit.SECOND -> TimeUnitProto.SECOND
    TimeUnit.MILLISECOND -> TimeUnitProto.MILLI
    TimeUnit.MICROSECOND -> TimeUnitProto.MICRO
    TimeUnit.NANOSECOND -> TimeUnitProto.NANO
}

private fun TimeUnitProto.asTimeUnit() = when (this) {
    TimeUnitProto.SECOND -> TimeUnit.SECOND
    TimeUnitProto.MILLI -> TimeUnit.MILLISECOND
    TimeUnitProto.MICRO -> TimeUnit.MICROSECOND
    TimeUnitProto.NANO -> TimeUnit.NANOSECOND
    else -> throw Unsupported("unknown time unit: $this", "xtdb/col-meta-time-unit")
}

private fun DateUnit.toProto() = when (this) {
    DateUnit.DAY -> DateProto.Unit.DAY
    DateUnit.MILLISECOND -> DateProto.Unit.MILLI
}

private fun DateProto.Unit.asDateUnit() = when (this) {
    DateProto.Unit.DAY -> DateUnit.DAY
    DateProto.Unit.MILLI -> DateUnit.MILLISECOND
    else -> throw Unsupported("unknown date unit: $this", "xtdb/col-meta-date-unit")
}

private fun IntervalUnit.toProto() = when (this) {
    IntervalUnit.YEAR_MONTH -> IntervalProto.Unit.YEAR_MONTH
    IntervalUnit.DAY_TIME -> IntervalProto.Unit.DAY_TIME
    IntervalUnit.MONTH_DAY_NANO -> IntervalProto.Unit.MONTH_DAY_NANO
}

private fun IntervalProto.Unit.asArrowType(): ArrowType = when (this) {
    IntervalProto.Unit.YEAR_MONTH -> ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    IntervalProto.Unit.DAY_TIME -> ArrowType.Interval(IntervalUnit.DAY_TIME)
    IntervalProto.Unit.MONTH_DAY_NANO -> ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
    IntervalProto.Unit.MONTH_DAY_MICRO -> IntervalMDMType
    else -> throw Unsupported("unknown interval unit: $this", "xtdb/col-meta-interval-unit")
}

private fun Simple.asArrowType(): ArrowType = when (this) {
    Simple.BOOL -> ArrowType.Bool()
    Simple.I8 -> ArrowType.Int(8, true)
    Simple.I16 -> ArrowType.Int(16, true)
    Simple.I32 -> ArrowType.Int(32, true)
    Simple.I64 -> ArrowType.Int(64, true)
    Simple.F32 -> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    Simple.F64 -> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    Simple.UTF8 -> ArrowType.Utf8()
    Simple.VAR_BINARY -> ArrowType.Binary()
    Simple.KEYWORD -> KeywordType
    Simple.TRANSIT -> TransitType
    Simple.URI -> UriType
    Simple.UUID -> UuidType
    Simple.OID -> OidType
    Simple.REG_CLASS -> RegClassType
    Simple.REG_PROC -> RegProcType
    else -> throw Unsupported("not a scalar type: $this", "xtdb/col-meta-simple-type")
}

private fun VecType.Builder.setScalar(arrowType: ArrowType) = apply {
    when (val at = arrowType) {
        KeywordType -> simple = Simple.KEYWORD
        TransitType -> simple = Simple.TRANSIT
        UriType -> simple = Simple.URI
        UuidType -> simple = Simple.UUID
        OidType -> simple = Simple.OID
        RegClassType -> simple = Simple.REG_CLASS
        RegProcType -> simple = Simple.REG_PROC

        IntervalMDMType ->
            interval = IntervalProto.newBuilder().setUnit(IntervalProto.Unit.MONTH_DAY_MICRO).build()

        is ArrowType.Bool -> simple = Simple.BOOL
        is ArrowType.Utf8 -> simple = Simple.UTF8
        is ArrowType.Binary -> simple = Simple.VAR_BINARY

        is ArrowType.Int ->
            simple = when {
                !at.isSigned -> throw Unsupported("unsigned ints", "xtdb/col-meta-unsigned-int")
                at.bitWidth == 8 -> Simple.I8
                at.bitWidth == 16 -> Simple.I16
                at.bitWidth == 32 -> Simple.I32
                at.bitWidth == 64 -> Simple.I64
                else -> throw Unsupported("int width ${at.bitWidth}", "xtdb/col-meta-int-width")
            }

        is ArrowType.FloatingPoint ->
            simple = when (at.precision) {
                FloatingPointPrecision.SINGLE -> Simple.F32
                FloatingPointPrecision.DOUBLE -> Simple.F64
                else -> throw Unsupported("float precision ${at.precision}", "xtdb/col-meta-float-precision")
            }

        is ArrowType.FixedSizeBinary -> fixedSizeBinary = at.byteWidth

        is ArrowType.Decimal ->
            decimal = Decimal.newBuilder()
                .setPrecision(at.precision).setScale(at.scale).setBitWidth(at.bitWidth).build()

        is ArrowType.Timestamp ->
            timestamp = TimestampProto.newBuilder()
                .setUnit(at.unit.toProto())
                .also { b -> at.timezone?.let { b.timezone = it } }
                .build()

        is ArrowType.Date -> date = DateProto.newBuilder().setUnit(at.unit.toProto()).build()
        is ArrowType.Time -> time = TimeProto.newBuilder().setUnit(at.unit.toProto()).setBitWidth(at.bitWidth).build()
        is ArrowType.Duration -> duration = at.unit.toProto()
        is ArrowType.Interval -> interval = IntervalProto.newBuilder().setUnit(at.unit.toProto()).build()

        else -> throw Unsupported("cannot record arrow type: $at", "xtdb/col-meta-arrow-type")
    }
}

private fun VecType.Builder.setListy(arrowType: ArrowType, el: ColumnMeta) = apply {
    val elProto = el.toProto()

    when (val at = arrowType) {
        SetType -> set = Listy.newBuilder().setEl(elProto).build()
        TsTzRangeType -> tstzRange = Listy.newBuilder().setEl(elProto).build()
        is ArrowType.List -> list = Listy.newBuilder().setEl(elProto).build()

        is ArrowType.FixedSizeList ->
            fixedSizeList = FixedSizeList.newBuilder().setSize(at.listSize).setEl(elProto).build()

        is ArrowType.Map -> map = MapType.newBuilder().setKeysSorted(at.keysSorted).setEl(elProto).build()

        else -> throw Unsupported("not a listy arrow type: $at", "xtdb/col-meta-listy-type")
    }
}

/**
 * The leg this type would be written under, and so the order legs are recorded in — [ColumnMeta.Type.Poly]
 * holds a set, which has none of its own.
 */
private val ColumnMeta.Type.Mono.legName: String
    get() = when (this) {
        ColumnMeta.Type.Null -> ArrowType.Null().toLeg()
        is ColumnMeta.Type.Scalar -> arrowType.toLeg()
        is ColumnMeta.Type.Listy -> arrowType.toLeg()
        is ColumnMeta.Type.Struct -> ArrowType.Struct().toLeg()
    }

private fun ColumnMeta.Type.toProto(): VecType = VecType.newBuilder().apply {
    when (val t = this@toProto) {
        ColumnMeta.Type.Nothing -> simple = Simple.NOTHING
        ColumnMeta.Type.Null -> simple = Simple.NULL
        is ColumnMeta.Type.Scalar -> setScalar(t.arrowType)
        is ColumnMeta.Type.Listy -> setListy(t.arrowType, t.el)

        is ColumnMeta.Type.Struct ->
            struct = Struct.newBuilder().putAllChildren(t.children.mapValues { it.value.toProto() }).build()

        is ColumnMeta.Type.Maybe -> maybe = Maybe.newBuilder().setMono(t.mono.toProto()).build()

        is ColumnMeta.Type.Poly ->
            poly = Poly.newBuilder().addAllLegs(t.legs.sortedBy { it.legName }.map { it.toProto() }).build()
    }
}.build()

private fun VecType.asMono(): ColumnMeta.Type.Mono =
    asType() as? ColumnMeta.Type.Mono
        ?: throw Unsupported("union leg is not a mono type", "xtdb/col-meta-leg-not-mono")

private fun VecType.asListy(arrowType: ArrowType, el: ColumnMetaProto) =
    ColumnMeta.Type.Listy(arrowType, ColumnMeta.fromProto(elsName(arrowType), el))

private fun VecType.asType(): ColumnMeta.Type = when (typeCase) {
    TypeCase.SIMPLE -> when (simple) {
        Simple.NOTHING -> ColumnMeta.Type.Nothing
        Simple.NULL -> ColumnMeta.Type.Null
        else -> ColumnMeta.Type.Scalar(simple.asArrowType())
    }

    TypeCase.FIXED_SIZE_BINARY -> ColumnMeta.Type.Scalar(ArrowType.FixedSizeBinary(fixedSizeBinary))
    TypeCase.DECIMAL -> ColumnMeta.Type.Scalar(ArrowType.Decimal(decimal.precision, decimal.scale, decimal.bitWidth))

    TypeCase.TIMESTAMP ->
        ColumnMeta.Type.Scalar(
            ArrowType.Timestamp(timestamp.unit.asTimeUnit(), timestamp.timezone.takeIf { timestamp.hasTimezone() })
        )

    TypeCase.DATE -> ColumnMeta.Type.Scalar(ArrowType.Date(date.unit.asDateUnit()))
    TypeCase.TIME -> ColumnMeta.Type.Scalar(ArrowType.Time(time.unit.asTimeUnit(), time.bitWidth))
    TypeCase.DURATION -> ColumnMeta.Type.Scalar(ArrowType.Duration(duration.asTimeUnit()))
    TypeCase.INTERVAL -> ColumnMeta.Type.Scalar(interval.unit.asArrowType())

    TypeCase.LIST -> asListy(ArrowType.List(), list.el)
    TypeCase.SET -> asListy(SetType, set.el)
    TypeCase.TSTZ_RANGE -> asListy(TsTzRangeType, tstzRange.el)
    TypeCase.FIXED_SIZE_LIST -> asListy(ArrowType.FixedSizeList(fixedSizeList.size), fixedSizeList.el)
    TypeCase.MAP -> asListy(ArrowType.Map(map.keysSorted), map.el)

    TypeCase.STRUCT ->
        ColumnMeta.Type.Struct(struct.childrenMap.mapValues { (name, col) -> ColumnMeta.fromProto(name, col) })

    TypeCase.MAYBE -> ColumnMeta.Type.Maybe(maybe.mono.asMono())
    TypeCase.POLY -> ColumnMeta.Type.Poly(poly.legsList.mapTo(mutableSetOf()) { it.asMono() })

    else -> throw Unsupported("no column type recorded", "xtdb/col-meta-type-absent")
}

/**
 * The type this node and its children describe.
 *
 * Reassembled on demand rather than held: the two are the same information, so storing both makes a
 * descendant's type something a parent has to be kept in step with.
 */
val ColumnMeta.Type.vectorType: VectorType
    get() = when (this) {
        ColumnMeta.Type.Nothing -> VectorType.Nothing
        ColumnMeta.Type.Null -> VectorType.Null
        is ColumnMeta.Type.Scalar -> VectorType.Scalar(arrowType)
        is ColumnMeta.Type.Listy -> VectorType.Listy(arrowType, el.type.vectorType)
        is ColumnMeta.Type.Struct -> VectorType.Struct(children.mapValues { it.value.type.vectorType })
        is ColumnMeta.Type.Maybe -> VectorType.maybe(mono.vectorType)
        is ColumnMeta.Type.Poly -> VectorType.fromLegs(legs.map { it.vectorType })
    }

/**
 * An ordinal for each of [this], taken from [prev] where it holds one and appended after the highest it
 * holds where it doesn't.
 *
 * New names are numbered in name order rather than iteration order: every node folds the same blocks and
 * has to reach the same numbering, and a map has no order of its own to inherit.
 */
internal fun Iterable<FieldName>.ordinalsFrom(prev: Map<FieldName, Int>): Map<FieldName, Int> {
    var next = prev.values.maxOrNull()?.plus(1) ?: 0

    return sorted().associateWith { prev[it] ?: next++ }
}

internal fun Map<FieldName, ColumnMeta>.withOrdinalsFrom(
    prev: Map<FieldName, ColumnMeta>
): Map<FieldName, ColumnMeta> {
    val ordinals = keys.ordinalsFrom(prev.mapValues { it.value.ordinal })

    return ordinals.mapValues { (name, ordinal) ->
        val col = getValue(name)
        col.copy(ordinal = ordinal, type = col.type.withOrdinalsFrom(prev[name]?.type))
    }
}

private fun ColumnMeta.Type.Mono.withOrdinalsFrom(prev: ColumnMeta.Type.Mono?): ColumnMeta.Type.Mono = when (this) {
    is ColumnMeta.Type.Struct ->
        ColumnMeta.Type.Struct(children.withOrdinalsFrom((prev as? ColumnMeta.Type.Struct)?.children.orEmpty()))

    is ColumnMeta.Type.Listy -> {
        val prevEl = (prev as? ColumnMeta.Type.Listy)?.el
        ColumnMeta.Type.Listy(arrowType, el.copy(type = el.type.withOrdinalsFrom(prevEl?.type)))
    }

    else -> this
}

/**
 * The legs [this] holds, whatever it wrapped them in.
 *
 * Matching the wrapper instead — a previous [ColumnMeta.Type.Maybe] against this one — loses every ordinal
 * beneath a leg on the merge that widens it, which is the merge a column meets the first time a block
 * doesn't write it.
 */
private val ColumnMeta.Type?.legsByName: Map<String, ColumnMeta.Type.Mono>
    get() = when (this) {
        is ColumnMeta.Type.Mono -> mapOf(legName to this)
        is ColumnMeta.Type.Maybe -> mapOf(mono.legName to mono)
        is ColumnMeta.Type.Poly -> legs.associateBy { it.legName }
        else -> emptyMap()
    }

private fun ColumnMeta.Type.withOrdinalsFrom(prev: ColumnMeta.Type?): ColumnMeta.Type {
    val prevLegs = prev.legsByName

    return when (this) {
        ColumnMeta.Type.Nothing -> this
        is ColumnMeta.Type.Mono -> withOrdinalsFrom(prevLegs[legName])
        is ColumnMeta.Type.Maybe -> ColumnMeta.Type.Maybe(mono.withOrdinalsFrom(prevLegs[mono.legName]))

        is ColumnMeta.Type.Poly ->
            ColumnMeta.Type.Poly(legs.mapTo(mutableSetOf()) { it.withOrdinalsFrom(prevLegs[it.legName]) })
    }
}

private fun VectorType.Mono.asColType(): ColumnMeta.Type.Mono = when (this) {
    VectorType.Null -> ColumnMeta.Type.Null
    is VectorType.Scalar -> ColumnMeta.Type.Scalar(arrowType)
    is VectorType.Listy -> ColumnMeta.Type.Listy(arrowType, ColumnMeta.of(elsName(arrowType), elType))
    is VectorType.Struct -> ColumnMeta.Type.Struct(children.mapValues { (n, t) -> ColumnMeta.of(n, t) })
}

private fun VectorType.asColType(): ColumnMeta.Type = when (this) {
    VectorType.Nothing -> ColumnMeta.Type.Nothing
    is VectorType.Mono -> asColType()
    is VectorType.Maybe -> ColumnMeta.Type.Maybe(mono.asColType())
    is VectorType.Poly -> ColumnMeta.Type.Poly(legs.mapTo(mutableSetOf()) { it.asColType() })
}

/**
 * What the catalog knows about one position in a table's type tree: a top-level column, a nested struct
 * key, or a list's element type.
 *
 * [type] mirrors [VectorType], except that a [Type.Struct]'s children and a [Type.Listy]'s element hold a
 * [ColumnMeta] rather than a [VectorType]. Polymorphism recurses straight through — [Type.Poly]'s legs and
 * [Type.Maybe]'s mono are types rather than nodes — because a leg has neither a stable position to be
 * identified by ([VectorType.Poly] holds an unordered set that `fromLegs` normalises) nor a name of its own
 * that a rename could change.
 *
 * The consequence worth knowing at a call site: widening a type moves no nodes. `a: i64` becoming
 * `a: i64 | text` re-shapes `a`'s [type] and leaves everything else on the node where it was.
 *
 * @param slug the Arrow field name this position's data is written under, and the key it is held under —
 *   mirrored onto the node so that one can be passed around on its own. Fixed for the position's
 *   lifetime: changing it orphans everything already written under it. A list's element takes the slug
 *   its parent's arrow type fixes.
 * @param name the name a user sees, which a rename changes and [slug] does not. The two are the same
 *   string until there is a rename to tell them apart.
 * @param ordinal this position's place among its siblings, in the order they were first seen — dense,
 *   append-only, and never reused, so it does not move when a sibling is added.
 * @param hll the distinct-value estimate accumulated for this position, or null where none has been
 *   computed.
 */
data class ColumnMeta(
    val slug: FieldName,
    val name: FieldName,
    val ordinal: Int,
    val type: Type,
    val hll: HLL?
) {

    sealed interface Type {

        /** A type that may be a union leg — everything but [Maybe] and [Poly]. */
        sealed interface Mono : Type

        /** @see VectorType.Nothing */
        data object Nothing : Type

        /** @see VectorType.Null */
        data object Null : Mono

        data class Scalar(val arrowType: ArrowType) : Mono

        data class Listy(val arrowType: ArrowType, val el: ColumnMeta) : Mono

        data class Struct(val children: Map<FieldName, ColumnMeta>) : Mono

        data class Maybe(val mono: Mono) : Type

        data class Poly(val legs: Set<Mono>) : Type
    }

    fun toProto(): ColumnMetaProto =
        ColumnMetaProto.newBuilder()
            .setType(type.toProto())
            .setOrdinal(ordinal)
            .setName(name)
            .also { b -> hll?.let { b.hll = ByteString.copyFrom(it.duplicate()) } }
            .build()

    companion object {
        /**
         * A node for [type] at a position called [name], and one for each position beneath it.
         *
         * The slug is the name: a column arrives as a key in the put struct and its data is written under
         * that key, so there is nothing to normalise, unlike a table's.
         *
         * Every node takes ordinal 0: a tree built without one to compare against has no first-seen order
         * to recover, so ordinals are settled by [withOrdinalsFrom] against the tree this one folds into.
         */
        @JvmStatic
        @JvmOverloads
        fun of(name: FieldName, type: VectorType, hll: HLL? = null) =
            ColumnMeta(name, name, 0, type.asColType(), hll)

        /** A node held under [slug], taking [slug] as its name where the block predates the two being distinct. */
        @JvmStatic
        fun fromProto(slug: FieldName, proto: ColumnMetaProto) =
            ColumnMeta(
                slug, proto.name.takeIf { proto.hasName() } ?: slug,
                proto.ordinal, proto.type.asType(),
                proto.hll.takeIf { proto.hasHll() }?.let { toHLL(it.toByteArray()) }
            )
    }
}
