package xtdb.operator

import xtdb.api.error.Fault
import xtdb.arrow.RelationReader
import java.time.ZoneId

data class OrderSpec(val column: String, val direction: Direction, val nullOrdering: NullOrdering) {
    enum class Direction { ASC, DESC }
    enum class NullOrdering { NULLS_FIRST, NULLS_LAST }
}

private fun OrderSpec.comparator(left: RelationReader, right: RelationReader, zone: ZoneId): Comparator<Int> {
    val desc = direction == OrderSpec.Direction.DESC

    // DESC flips the null ordering before negating, so that NULLS FIRST/LAST stays put rather than moving with the direction (#6172).
    val cmp = left.vectorFor(column)
        .orderComparator(right.vectorFor(column), (nullOrdering == OrderSpec.NullOrdering.NULLS_FIRST) != desc, zone)

    return if (desc) Comparator { l, r -> -cmp.applyAsInt(l, r) }
    else Comparator { l, r -> cmp.applyAsInt(l, r) }
}

/**
 * Orders row `l` of [left] against row `r` of [right] by each of these specs in turn, comparing temporal values in [zone].
 * [left] and [right] both hold every spec's column, and may be the same relation.
 *
 * @throws IllegalStateException if either relation lacks a spec's column.
 * @throws Fault `xtdb/no-order-specs` if there are no specs.
 */
fun List<OrderSpec>.comparator(left: RelationReader, right: RelationReader, zone: ZoneId): Comparator<Int> {
    if (isEmpty()) throw Fault("no order specs to compare rows by", "xtdb/no-order-specs")
    return map { it.comparator(left, right, zone) }.reduce { acc, cmp -> acc.thenComparing(cmp) }
}

/** This relation's row indices, sorted by [specs] as [comparator] orders them. */
fun RelationReader.sortedIdxs(specs: List<OrderSpec>, zone: ZoneId): IntArray =
    (0 until rowCount).sortedWith(specs.comparator(this, this, zone)).toIntArray()
