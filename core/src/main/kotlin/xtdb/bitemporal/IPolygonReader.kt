package xtdb.bitemporal

import kotlin.math.max
import kotlin.math.min

interface IPolygonReader {
    val validTimeRangeCount: Int

    fun getValidFrom(rangeIdx: Int): Long

    fun getValidTo(rangeIdx: Int): Long

    fun getSystemTo(rangeIdx: Int): Long

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
}
