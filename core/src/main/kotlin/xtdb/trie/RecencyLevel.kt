package xtdb.trie

import xtdb.time.MICRO_HZ

/**
 * Within a historical file, a trie that branches on recency interleaves it with IID on a fixed schedule:
 * IID, [DAY], IID, [SIX_HOURS], IID, [HOUR], then IID only.
 */
enum class RecencyLevel(private val widthMicros: Long) {
    DAY(24L * 60 * 60 * MICRO_HZ),
    SIX_HOURS(6L * 60 * 60 * MICRO_HZ),
    HOUR(60L * 60 * MICRO_HZ);

    /**
     * The inclusive upper bound of [recency]'s bucket at this level - the same convention as a file's recency.
     */
    fun bucketFor(recency: RecencyMicros): RecencyMicros = (Math.floorDiv(recency - 1, widthMicros) + 1) * widthMicros

    companion object {
        /**
         * The level a node branches on, if any - given its IID depth, the recency levels above it,
         * and the IID levels fixed by the file's part, which no recency branch sits among.
         */
        @JvmStatic
        fun scheduled(iidDepth: Int, levelsUsed: Int, pinnedDepth: Int): RecencyLevel? =
            entries.getOrNull(levelsUsed)?.takeIf { iidDepth >= pinnedDepth && iidDepth > levelsUsed }
    }
}
