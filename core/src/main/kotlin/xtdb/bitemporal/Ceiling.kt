package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList
import java.util.*

@JvmRecord
data class Ceiling(val validTimes: LongArrayList, val sysTimeCeilings: LongArrayList) {
    constructor() : this(LongArrayList(), LongArrayList()) {
        reset()
    }

    fun getValidFrom(rangeIdx: Int) = validTimes[rangeIdx]

    fun getValidTo(rangeIdx: Int) = validTimes[rangeIdx + 1]

    fun getSystemTime(rangeIdx: Int) = sysTimeCeilings[rangeIdx]

    @Suppress("MemberVisibilityCanBePrivate")
    fun reset() {
        validTimes.clear()
        validTimes.add(Long.MIN_VALUE, Long.MAX_VALUE)

        sysTimeCeilings.clear()
        sysTimeCeilings.add(Long.MAX_VALUE)
    }

    fun applyLog(systemFrom: Long, validFrom: Long, validTo: Long) {
        if (validFrom >= validTo) return

        var end = Arrays.binarySearch(validTimes.buffer, 0, validTimes.elementsCount, validTo)

        if (end < 0) {
            end = -(end + 1)
            validTimes.insert(end, validTo)
            sysTimeCeilings.insert(end, sysTimeCeilings[end - 1])
        }

        var start = Arrays.binarySearch(validTimes.buffer, 0, validTimes.elementsCount, validFrom)
        val insertedStart = start < 0
        start = if (insertedStart) -(start + 1) else start

        if (insertedStart && start == end) {
            // can't overwrite the value this time as this is already our validTo, so we insert
            validTimes.insert(start, validFrom)
            sysTimeCeilings.insert(start, systemFrom)

            // we've shifted everything one to the right, so increment end
            end++
        } else {
            validTimes[start] = validFrom
            sysTimeCeilings[start] = systemFrom
        }

        // delete all the ranges strictly between our start and our end
        if (start + 1 < end) {
            validTimes.removeRange(start + 1, end)
            sysTimeCeilings.removeRange(start + 1, end)
        }
    }
}
