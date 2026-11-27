@file:OptIn(InternalApi::class)

package xtdb.arrow

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.InternalApi
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.time.Interval
import xtdb.time.TemporalMath
import xtdb.time.hz
import xtdb.time.ZoneOffsets
import java.time.ZoneId
import java.util.function.IntBinaryOperator
import java.util.function.IntToLongFunction
import java.util.function.IntUnaryOperator

internal const val TRUE_3VL = 1
internal const val FALSE_3VL = -1
internal const val NULL_3VL = 0

internal fun b3(b: Boolean) = if (b) TRUE_3VL else FALSE_3VL

private val ALWAYS_FALSE = IntBinaryOperator { _, _ -> FALSE_3VL }

// a null leg's rows are null, so the null check above the leaves answers for them first
private val NULL_LEG = IntBinaryOperator { _, _ -> throw Fault("compared a null leg's row", "xtdb.arrow/null-leg-compared") }

internal fun equiRowComparator(l: VectorReader, r: VectorReader, nullsEqual: Boolean, zone: ZoneId): IntBinaryOperator {
    val values = rowPairs(l, r) { lv, rv -> equiLeaf(lv, rv, zone) }
    return if (nullsEqual) withNulls(l, r, TRUE_3VL, FALSE_3VL, FALSE_3VL, values)
    else withNulls(l, r, NULL_3VL, NULL_3VL, NULL_3VL, values)
}

internal fun nestedEquiComparator(l: VectorReader, r: VectorReader, zone: ZoneId): IntBinaryOperator =
    withNulls(l, r, NULL_3VL, NULL_3VL, NULL_3VL, rowPairs(l, r) { lv, rv -> equiLeaf(lv, rv, zone) })

internal fun orderRowComparator(l: VectorReader, r: VectorReader, nullsFirst: Boolean, zone: ZoneId): IntBinaryOperator {
    val values = rowPairs(l, r) { lv, rv -> orderLeaf(lv, rv, zone) }
    val nullCmp = if (nullsFirst) -1 else 1
    return withNulls(l, r, 0, nullCmp, -nullCmp, values)
}

private fun withNulls(
    l: VectorReader, r: VectorReader, bothNull: Int, leftNull: Int, rightNull: Int, values: IntBinaryOperator
) = IntBinaryOperator { lIdx, rIdx ->
    if (l.isNull(lIdx)) {
        if (r.isNull(rIdx)) bothNull else leftNull
    } else if (r.isNull(rIdx)) rightNull
    else values.applyAsInt(lIdx, rIdx)
}

private fun equiLeaf(l: Vector, r: Vector, zone: ZoneId): IntBinaryOperator = when {
    l is NullVector || r is NullVector -> NULL_LEG
    else -> l.equiRows(r, zone) ?: r.equiRows(l, zone)?.let { flipped -> IntBinaryOperator { lIdx, rIdx -> flipped.applyAsInt(rIdx, lIdx) } } ?: ALWAYS_FALSE
}

private fun orderLeaf(l: Vector, r: Vector, zone: ZoneId): IntBinaryOperator = when {
    l is NullVector || r is NullVector -> NULL_LEG
    else -> l.compareRows(r, zone)
        ?: throw Incorrect(
            "compare not applicable to types ${l.arrowType.toLeg()} and ${r.arrowType.toLeg()}",
            "xtdb.expression/function-type-mismatch"
        )
}

private fun IntBinaryOperator.mapLeft(f: IntUnaryOperator) =
    IntBinaryOperator { lIdx, rIdx -> applyAsInt(f.applyAsInt(lIdx), rIdx) }

private fun IntBinaryOperator.mapRight(f: IntUnaryOperator) =
    IntBinaryOperator { lIdx, rIdx -> applyAsInt(lIdx, f.applyAsInt(rIdx)) }

/**
 * A comparator per leg of [union], built for every leg it has now, and for any leg it gains later when one of its rows is first compared -
 * a union written to after the comparator is built can gain legs.
 */
private class UnionLegs(private val union: DenseUnionVector, private val build: (Vector) -> IntBinaryOperator) {
    private var legs = Array(union.legCount) { build(union.legAt(it)) }

    operator fun get(typeId: Int): IntBinaryOperator {
        if (typeId >= legs.size) legs = Array(union.legCount) { legs.getOrNull(it) ?: build(union.legAt(it)) }
        return legs[typeId]
    }
}

/**
 * Reduces [l] and [r] to the plain vectors beneath them - through selections, union legs and run-end encoding -
 * and has [leaf] compare each pair of those, called only for rows neither of which is null.
 */
private fun rowPairs(l: VectorReader, r: VectorReader, leaf: (Vector, Vector) -> IntBinaryOperator): IntBinaryOperator =
    when {
        l is IndirectVector -> rowPairs(l.inner, r, leaf).mapLeft(l.sel::getIndex)
        l is DenseUnionVector.LegReader -> rowPairs(l.inner, r, leaf).mapLeft(l::getOffset)
        l is DenseUnionVector.LegVector -> rowPairs(l.reader, r, leaf)
        l is RunEndEncodedVector -> rowPairs(l.runValues, r, leaf).mapLeft(l::runIdx)

        r is IndirectVector -> rowPairs(l, r.inner, leaf).mapRight(r.sel::getIndex)
        r is DenseUnionVector.LegReader -> rowPairs(l, r.inner, leaf).mapRight(r::getOffset)
        r is DenseUnionVector.LegVector -> rowPairs(l, r.reader, leaf)
        r is RunEndEncodedVector -> rowPairs(l, r.runValues, leaf).mapRight(r::runIdx)

        l is DenseUnionVector -> {
            val legs = UnionLegs(l) { rowPairs(it, r, leaf) }
            IntBinaryOperator { lIdx, rIdx -> legs[l.getTypeId(lIdx).toInt()].applyAsInt(l.getOffset(lIdx), rIdx) }
        }

        r is DenseUnionVector -> {
            val legs = UnionLegs(r) { rowPairs(l, it, leaf) }
            IntBinaryOperator { lIdx, rIdx -> legs[r.getTypeId(rIdx).toInt()].applyAsInt(lIdx, r.getOffset(rIdx)) }
        }

        l is Vector && r is Vector -> leaf(l, r)

        else -> throw Unsupported("can't compare ${l::class.simpleName} with ${r::class.simpleName}", "xtdb.arrow/uncomparable-readers")
    }

internal fun compareTicks(x: Long, xHz: Long, y: Long, yHz: Long): Int {
    if (xHz == yHz) return x.compareTo(y)
    val c = Math.floorDiv(x, xHz).compareTo(Math.floorDiv(y, yHz))
    return if (c != 0) c else (Math.floorMod(x, xHz) * (1_000_000_000 / xHz)).compareTo(Math.floorMod(y, yHz) * (1_000_000_000 / yHz))
}

/**
 * A date or timestamp's rows as ticks at [hz]: [local] for a wall-clock reading, else an instant.
 * A date reads as the local midnight starting it.
 */
internal class DateTimeTicks(val local: Boolean, val hz: Long, val ticks: IntToLongFunction)

internal fun compareDateTimes(l: DateTimeTicks, r: DateTimeTicks, zone: ZoneOffsets): IntBinaryOperator {
    val lHz = l.hz
    val rHz = r.hz
    return when {
        l.local == r.local -> IntBinaryOperator { lIdx, rIdx -> compareTicks(l.ticks.applyAsLong(lIdx), lHz, r.ticks.applyAsLong(rIdx), rHz) }

        l.local -> IntBinaryOperator { lIdx, rIdx ->
            compareTicks(l.ticks.applyAsLong(lIdx), lHz, TemporalMath.toLocal(r.ticks.applyAsLong(rIdx), rHz, zone), rHz)
        }

        else -> IntBinaryOperator { lIdx, rIdx ->
            compareTicks(TemporalMath.toLocal(l.ticks.applyAsLong(lIdx), lHz, zone), lHz, r.ticks.applyAsLong(rIdx), rHz)
        }
    }
}

internal fun IntBinaryOperator.asEqui() = IntBinaryOperator { lIdx, rIdx -> b3(applyAsInt(lIdx, rIdx) == 0) }

/**
 * Bytes compare unsigned, byte by byte.
 * A string compares as its UTF-8 encoding, a UUID as its 16 bytes.
 */
internal fun compareBytes(l: Vector, r: Vector): IntBinaryOperator {
    val lPtr = ArrowBufPointer()
    val rPtr = ArrowBufPointer()
    return IntBinaryOperator { lIdx, rIdx -> l.getPointer(lIdx, lPtr).compareTo(r.getPointer(rIdx, rPtr)) }
}

internal fun equiBytes(l: Vector, r: Vector): IntBinaryOperator {
    val lPtr = ArrowBufPointer()
    val rPtr = ArrowBufPointer()
    return IntBinaryOperator { lIdx, rIdx -> b3(l.getPointer(lIdx, lPtr) == r.getPointer(rIdx, rPtr)) }
}

/**
 * The vectors whose values are compared for `=` as their bytes, whatever their types - so `'foo' = :foo`.
 * A URI isn't one of them: it compares as a [java.net.URI].
 */
internal val Vector.isBytes get() =
    this is VariableWidthVector || this is FixedSizeBinaryVector || this is KeywordVector || this is UuidVector

internal fun equiObjects(l: Vector, r: Vector) =
    IntBinaryOperator { lIdx, rIdx -> b3(l.getObject(lIdx) == r.getObject(rIdx)) }

internal fun and3(l: IntBinaryOperator, r: IntBinaryOperator) = IntBinaryOperator { lIdx, rIdx ->
    val lRes = l.applyAsInt(lIdx, rIdx)
    if (lRes == FALSE_3VL) FALSE_3VL else minOf(lRes, r.applyAsInt(lIdx, rIdx))
}

internal fun equiLists(l: Vector, r: Vector, zone: ZoneId): IntBinaryOperator {
    val els = nestedEquiComparator(l.listElements, r.listElements, zone)
    return IntBinaryOperator { lIdx, rIdx ->
        val len = l.getListCount(lIdx)
        if (len != r.getListCount(rIdx)) return@IntBinaryOperator FALSE_3VL

        val lStart = l.getListStartIndex(lIdx)
        val rStart = r.getListStartIndex(rIdx)
        var res = TRUE_3VL
        for (n in 0 until len) {
            res = minOf(res, els.applyAsInt(lStart + n, rStart + n))
            if (res == FALSE_3VL) break
        }
        res
    }
}

private val Vector.dateTimeTicks: DateTimeTicks?
    get() = when (this) {
        is DateDayVector -> DateTimeTicks(true, 1L) { getInt(it) * SECS_PER_DAY }
        is DateMilliVector -> DateTimeTicks(true, 1L) { getEpochDay(it) * SECS_PER_DAY }
        is TimestampLocalVector -> DateTimeTicks(true, unit.hz.toLong()) { getLong(it) }
        is TimestampTzVector -> DateTimeTicks(false, unit.hz.toLong()) { getLong(it) }
        else -> null
    }

internal fun compareDateTimeRows(l: Vector, r: Vector, zone: ZoneId): IntBinaryOperator? {
    val lTicks = l.dateTimeTicks ?: return null
    val rTicks = r.dateTimeTicks ?: return null
    return compareDateTimes(lTicks, rTicks, ZoneOffsets.of(zone))
}

private val Vector.timeTicks: DateTimeTicks?
    get() = when (this) {
        is Time32Vector -> DateTimeTicks(true, unit.hz.toLong()) { getInt(it).toLong() }
        is Time64Vector -> DateTimeTicks(true, unit.hz.toLong()) { getLong(it) }
        is DurationVector -> DateTimeTicks(true, unit.hz.toLong()) { getLong(it) }
        else -> null
    }

internal fun compareTimeRows(l: Vector, r: Vector): IntBinaryOperator? {
    if ((l is DurationVector) != (r is DurationVector)) return null
    val lTicks = l.timeTicks ?: return null
    val rTicks = r.timeTicks ?: return null
    val lHz = lTicks.hz
    val rHz = rTicks.hz
    return IntBinaryOperator { lIdx, rIdx -> compareTicks(lTicks.ticks.applyAsLong(lIdx), lHz, rTicks.ticks.applyAsLong(rIdx), rHz) }
}

internal val Vector.isInterval get() =
    this is IntervalYearMonthVector || this is IntervalDayTimeVector || this is IntervalMonthDayNanoVector || this is IntervalMonthDayMicroVector

private const val SECS_PER_DAY = 86_400L
private const val NANOS_PER_DAY = SECS_PER_DAY * 1_000_000_000

private fun compareMdIntervals(l: Interval, r: Interval): Int {
    if (l.months > 0 || r.months > 0)
        throw Incorrect(
            "Cannot compare month-day-micro/nano intervals when month component is non-zero.",
            "xtdb.expression/cannot-compare-mdn-interval-with-months"
        )

    return Math.addExact(Math.multiplyExact(l.days.toLong(), NANOS_PER_DAY), l.nanos)
        .compareTo(Math.addExact(Math.multiplyExact(r.days.toLong(), NANOS_PER_DAY), r.nanos))
}

/** Intervals order only against intervals of the same unit, and a day-time interval not at all. */
internal fun compareIntervals(l: Vector, r: Vector): IntBinaryOperator? {
    if (!r.isInterval) return null
    if (l::class != r::class)
        throw Incorrect("Cannot compare intervals with different units", "xtdb.expression/cannot-compare-different-unit-intervals")

    return when (l) {
        is IntervalYearMonthVector -> IntBinaryOperator { lIdx, rIdx -> l.getInt(lIdx).compareTo(r.getInt(rIdx)) }
        is IntervalMonthDayNanoVector, is IntervalMonthDayMicroVector ->
            IntBinaryOperator { lIdx, rIdx -> compareMdIntervals(l.getObject(lIdx) as Interval, r.getObject(rIdx) as Interval) }
        else -> null
    }
}
