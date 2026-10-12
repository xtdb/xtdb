package xtdb.trie

/**
 * The smallest and largest `_recency` of some rows, inclusive; [EMPTY] for none.
 */
data class RecencyRange(val min: RecencyMicros, val max: RecencyMicros) {

    operator fun plus(other: RecencyRange) = RecencyRange(minOf(min, other.min), maxOf(max, other.max))

    val splittable get() = min < max

    /**
     * Which of [k] equal-width parts of this range [recency] falls into, from 0 for the oldest.
     */
    fun partIdx(recency: RecencyMicros, k: Int): Int = ((recency - min) / ((max - min) / k + 1)).toInt()

    companion object {
        @JvmField
        val EMPTY = RecencyRange(Long.MAX_VALUE, Long.MIN_VALUE)

        /**
         * How many parts a node of [rowCount] rows splits its recency range into:
         * four when its parts will branch again, otherwise enough for a page each.
         */
        fun splitCount(rowCount: Int, pageSize: Int): Int =
            if (rowCount > 4L * pageSize) 4 else (rowCount + pageSize - 1) / pageSize
    }
}
