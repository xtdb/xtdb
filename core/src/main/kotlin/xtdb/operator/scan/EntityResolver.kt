package xtdb.operator.scan

import xtdb.bitemporal.PolygonCalculator
import xtdb.util.TemporalBounds
import kotlin.math.max
import kotlin.math.min

/** Resolves one entity's events into the rows the query observes. */
internal interface EntityResolver {
    fun resolveEntity(events: EntityEvents, out: BitemporalConsumer)
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
