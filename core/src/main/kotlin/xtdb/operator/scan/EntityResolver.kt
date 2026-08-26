package xtdb.operator.scan

import xtdb.bitemporal.PolygonCalculator
import xtdb.util.TemporalBounds
import kotlin.math.max
import kotlin.math.min

/** Resolves one entity's events into the rows the query observes. */
internal interface EntityResolver {
    fun resolveEntity(events: EntityEvents, out: BitemporalConsumer)
}

/**
 * Resolves a query that is `AS OF` in both dimensions, where an entity's visible row is the newest
 * event below the system-time bound whose valid-time range covers the query's valid-time point.
 *
 * Nothing seen before that winner can cover the point — it would have been the winner — so every
 * earlier event lies wholly to one side of it, and the ceiling [PolygonResolver] maintains collapses
 * to the two scalars bracketing the gap the winner shows through. `_system_to` is null for the same
 * reason: the ceiling only drops below `MAX_VALUE` over a covering event's range.
 */
internal class AsOfResolver(
    temporalBounds: TemporalBounds, private val clampValidTime: Boolean
) : EntityResolver {

    private val vtLower = temporalBounds.validTime.lower
    private val vtUpper = temporalBounds.validTime.upper
    private val stUpper = temporalBounds.systemTime.upper

    override fun resolveEntity(events: EntityEvents, out: BitemporalConsumer) {
        var leftBound = Long.MIN_VALUE
        var rightBound = Long.MAX_VALUE

        while (true) {
            val leafPtr = events.nextEvent() ?: return
            val evPtr = leafPtr.evPtr
            val op = evPtr.op

            // an erase is exempt from the system-time bound, and suppresses only events older than
            // itself — which, walking newest-first, are exactly the ones we have yet to reach.
            if (op == "erase") return

            if (evPtr.systemFrom >= stUpper) continue

            val validFrom = evPtr.validFrom
            val validTo = evPtr.validTo

            if (validFrom <= vtLower && vtLower < validTo) {
                if (op == "put") {
                    val outValidFrom = max(validFrom, leftBound)
                    val outValidTo = min(validTo, rightBound)

                    out.accept(
                        leafPtr.relIdx, evPtr.index,
                        if (clampValidTime) max(outValidFrom, vtLower) else outValidFrom,
                        if (clampValidTime) min(outValidTo, vtUpper) else outValidTo,
                        evPtr.systemFrom, Long.MAX_VALUE
                    )
                }

                return
            }

            // an empty range contributes no ceiling breakpoint, so it must not move the bounds
            if (validFrom < validTo) {
                if (validTo <= vtLower) leftBound = max(leftBound, validTo)
                else rightBound = min(rightBound, validFrom)
            }
        }
    }
}

internal class PolygonResolver(
    private val temporalBounds: TemporalBounds, private val clampValidTime: Boolean
) : EntityResolver {

    private val polygonCalculator = PolygonCalculator(temporalBounds)

    private val vtLower = temporalBounds.validTime.lower
    private val vtUpper = temporalBounds.validTime.upper

    override fun resolveEntity(events: EntityEvents, out: BitemporalConsumer) {
        while (true) {
            val leafPtr = events.nextEvent() ?: return
            val evPtr = leafPtr.evPtr

            val polygon = polygonCalculator.calculate(evPtr)?.takeIf { evPtr.op == "put" } ?: continue

            val sysFrom = evPtr.systemFrom
            val idx = evPtr.index

            repeat(polygon.validTimeRangeCount) { i ->
                val validFrom = polygon.getValidFrom(i)
                val validTo = polygon.getValidTo(i)
                val sysTo = polygon.getSystemTo(i)

                if (
                    temporalBounds.intersects(validFrom, validTo, sysFrom, sysTo)
                    && validFrom != validTo && sysFrom != sysTo
                ) {
                    out.accept(
                        leafPtr.relIdx, idx,
                        if (clampValidTime) max(validFrom, vtLower) else validFrom,
                        if (clampValidTime) min(validTo, vtUpper) else validTo,
                        sysFrom, sysTo
                    )
                }
            }
        }
    }
}
