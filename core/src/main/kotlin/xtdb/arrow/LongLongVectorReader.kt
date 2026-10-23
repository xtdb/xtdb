package xtdb.arrow

import kotlin.math.min

interface LongLongVectorReader : VectorReader {
    fun getLongLongHigh(idx: Int): Long
    fun getLongLongLow(idx: Int): Long

    /**
     * The end (exclusive) of the run holding [idx] as this reader stores it: `idx + 1` for one that stores each row on its own.
     * Every row in `[idx, storedRunEnd(idx))` holds [idx]'s value, but the rows after it may hold that value too.
     *
     * Requires `0 <= idx < valueCount`.
     */
    fun storedRunEnd(idx: Int): Int = idx + 1

    private fun sameValueAt(idx: Int, high: Long, low: Long) =
        getLongLongHigh(idx) == high && getLongLongLow(idx) == low

    /**
     * The end (exclusive) of the run of rows from [idx] that hold [idx]'s value, capped at [limit].
     *
     * Requires `idx < limit <= valueCount`, and rows of equal value to be contiguous in `[idx, limit)` - as they are in an iid-sorted page.
     *
     * Gallops before bisecting because a run is usually short relative to the page: doubling finds the end of the run in log(run) reads, where bisecting `[idx, limit)` would take log(limit - idx).
     */
    fun equalRunEnd(idx: Int, limit: Int): Int {
        val high = getLongLongHigh(idx)
        val low = getLongLongLow(idx)

        var onRun = idx
        var step = 1
        while (onRun + step < limit && sameValueAt(onRun + step, high, low)) {
            onRun += step
            step *= 2
        }

        var pastRun = min(onRun + step, limit)
        while (pastRun - onRun > 1) {
            val mid = (onRun + pastRun) ushr 1
            if (sameValueAt(mid, high, low)) onRun = mid else pastRun = mid
        }

        return pastRun
    }
}
