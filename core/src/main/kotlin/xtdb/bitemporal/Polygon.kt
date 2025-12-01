package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList
import xtdb.trie.EventRowPointer
import kotlin.math.min
import kotlin.math.max

data class Polygon(val validTimes: LongArrayList, val sysTimeCeilings: LongArrayList) {
    constructor() : this(LongArrayList(), LongArrayList())

    val validTimeRangeCount
        get() = sysTimeCeilings.elementsCount

    fun getValidFrom(rangeIdx: Int) = validTimes[rangeIdx]

    fun getValidTo(rangeIdx: Int) = validTimes[rangeIdx + 1]

    fun getSystemTo(rangeIdx: Int) = sysTimeCeilings[rangeIdx]

    /**
     * maximum value T where row is invalid for all (valid-time, sys-time) where both valid-time >= T and sys-time >= T
     */
    val recency: Long
        get() {
            val validTimeRangeCount = validTimeRangeCount

            // largest recency we've seen so far
            var recency = Long.MIN_VALUE

            var validTo = getValidTo(validTimeRangeCount - 1)

            // start from the RHS
            for (i in (validTimeRangeCount - 1) downTo 0) {
                recency = max(recency, min(getSystemTo(i), validTo))

                val validFrom = getValidFrom(i)

                // it's never going to get bigger than this, stop early
                if (recency >= validFrom) return recency

                validTo = validFrom
            }

            return recency
        }

    fun calculateFor(ceiling: Ceiling, validFrom: Long, validTo: Long) {
        validTimes.clear()
        sysTimeCeilings.clear()

        var validTime = validFrom

        var ceilIdx = ceiling.getCeilingIndex(validFrom)

        do {
            var ceilValidTo: Long

            while (true) {
                ceilValidTo = ceiling.getValidTo(ceilIdx)
                if (ceilValidTo > validTime) break
                ceilIdx++
            }

            validTimes.add(validTime)
            sysTimeCeilings.add(ceiling.getSystemTime(ceilIdx))

            validTime = min(ceilValidTo, validTo)
        } while (validTime != validTo)

        validTimes.add(validTime)
    }

    fun calculateFor(ceiling: Ceiling, evPtr: EventRowPointer) {
        calculateFor(ceiling, evPtr.validFrom, evPtr.validTo)
    }
}
