package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList
import xtdb.util.SkipList

/**
 * searches a descending-sorted list for the last element greater than or equal to the needle
 *
 * @return the index of the element if found, otherwise `- (insertion point) - 1` of the element that would be inserted
 */
/*
 * we opt for a linear search here rather than binary because of the probability that the needle is close to the end -
 * we're scanning the events in reverse system-time order, so with vt≈tt our valid-from/valid-to are likely to be
 * older than any we've seen so far.
 */
internal fun LongArrayList.reverseLinearSearch(needle: Long): Int {
    var idx = elementsCount
    while (--idx >= 0) {
        val x = buffer[idx]
        when {
            x == needle -> return idx
            x > needle -> return -idx - 2
        }
    }

    return -1
}

internal fun LongArrayList.binarySearch(needle: Long): Int {
    var left = 0
    var right = elementsCount
    while (left < right) {
        val mid = (left + right) / 2
        val x = buffer[mid]
        when {
            x == needle -> return mid
            x > needle -> left = mid + 1
            else -> right = mid
        }
    }
    return -left - 1
}

internal fun SkipList<Long>.binarySearch(needle: Long): Int {
    var left = 0
    var right = size
    while (left < right) {
        val mid = (left + right) / 2
        val x = get(mid)
        when {
            x == needle -> return mid
            x > needle -> left = mid + 1
            else -> right = mid
        }
    }
    return -left - 1
}

data class Ceiling(val validTimes: SkipList<Long>, val sysTimeCeilings: SkipList<Long>) {
    constructor() : this(SkipList<Long>(), SkipList<Long>()) {
        reset()
    }

    private fun reverseIdx(idx: Int) = validTimes.size - 1 - idx

    fun getValidFrom(rangeIdx: Int) = validTimes[reverseIdx(rangeIdx)]

    fun getValidTo(rangeIdx: Int) = validTimes[reverseIdx(rangeIdx + 1)]

    fun getSystemTime(rangeIdx: Int) = sysTimeCeilings[reverseIdx(rangeIdx) - 1]

    /**
     * @return the index (in reverse order) such that `validTimes[reverseIdx(idx)] <= validTime < validTimes[reverseIdx(idx + 1)]`
     * or 0 if `validTime < validTimes[reverseIdx(0)]`
     * or `validTimes.elementsCount - 1` if `validTime >= validTimes[reverseIdx(validTimes.elementsCount - 1)]`
     */
    fun getCeilingIndex(validTime: Long): Int {
        var idx = validTimes.binarySearch(validTime)
        if (idx < 0) idx = -(idx + 1)
        if (idx < validTimes.size - 1 && validTime < validTimes[idx]) idx++
        // TODO this shouldn't be necessary
        if (idx == validTimes.size) idx--
        return reverseIdx(idx)
    }

    @Suppress("MemberVisibilityCanBePrivate")
    fun reset() {
        validTimes.clear()
        validTimes.add(Long.MAX_VALUE)
        validTimes.add(Long.MIN_VALUE)

        sysTimeCeilings.clear()
        sysTimeCeilings.add(Long.MAX_VALUE)
    }

    fun applyLog(systemFrom: Long, validFrom: Long, validTo: Long) {
        if (validFrom >= validTo) return

        var end = validTimes.binarySearch(validTo)
        val insertedEnd = end < 0
        if (insertedEnd) end = -(end + 1)

        var start = validTimes.binarySearch(validFrom)
        val insertedStart = start < 0
        if (insertedStart) start = -(start + 1)

        when {
            !insertedEnd && !insertedStart -> {
                sysTimeCeilings[end] = systemFrom
            }

            !insertedEnd -> {
                validTimes.insert(start, validFrom)
                sysTimeCeilings.insert(end, systemFrom)
            }

            !insertedStart -> {
                validTimes.insert(end, validTo)
                sysTimeCeilings.insert(end, systemFrom)
                start++
            }

            end == start -> {
                validTimes.insert(end, validTo)
                sysTimeCeilings.insert(end, systemFrom)
                start++
                validTimes.insert(start, validFrom)
                sysTimeCeilings.insert(start, sysTimeCeilings[end - 1])
            }

            else -> {
                validTimes.insert(end, validTo)
                sysTimeCeilings.insert(end, systemFrom)
                validTimes[start] = validFrom
            }
        }

        validTimes.removeRange(end + 1, start)
        sysTimeCeilings.removeRange(end + 1, start)
    }

}
