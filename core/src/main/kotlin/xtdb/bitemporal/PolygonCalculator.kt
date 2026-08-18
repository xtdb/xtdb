package xtdb.bitemporal

import xtdb.trie.EventRowPointer
import xtdb.util.TemporalBounds

class PolygonCalculator(private val queryBounds: TemporalBounds? = null) {
    private val ceiling = Ceiling()
    private val polygon = Polygon()

    // the iid we're currently resolving, and whether we've seen an erase for it.
    // the initial (0, 0) needn't be distinguishable from a genuine iid: the most it can cost us is the
    // ceiling reset on the first row, and the ceiling is constructed already reset.
    private var iidHigh = 0L
    private var iidLow = 0L
    private var erased = false

    fun reset() = ceiling.reset()

    fun calculate(erp: EventRowPointer): Polygon? {
        if (erp.iidHigh != iidHigh || erp.iidLow != iidLow) {
            iidHigh = erp.iidHigh
            iidLow = erp.iidLow
            erased = false
            ceiling.reset()
        }

        // after an erase, we don't process any more rows
        if (erased) return null

        val isErase = erp.op == "erase"

        val systemFrom = erp.systemFrom

        // unless it's an erase, we don't want to take any events after the query's snapshot time into account.
        if (!isErase && queryBounds != null && systemFrom >= queryBounds.systemTime.upper) return null

        val validFrom = erp.validFrom
        val validTo = erp.validTo

        polygon.calculateFor(ceiling, validFrom, validTo)
        ceiling.applyLog(systemFrom, validFrom, validTo)

        if (isErase) erased = true

        return polygon
    }
}
