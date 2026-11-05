package xtdb.trie

/**
 * The smallest and largest `_recency` of some rows, inclusive; [EMPTY] for none.
 */
data class RecencyRange(val min: RecencyMicros, val max: RecencyMicros) {

    operator fun plus(other: RecencyRange) = RecencyRange(minOf(min, other.min), maxOf(max, other.max))

    companion object {
        @JvmField
        val EMPTY = RecencyRange(Long.MAX_VALUE, Long.MIN_VALUE)
    }
}
