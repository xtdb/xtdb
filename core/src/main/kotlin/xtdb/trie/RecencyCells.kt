package xtdb.trie

/**
 * The time cells a historical file's recency branch splits into, below the IID levels its part pins.
 *
 * Cell widths come from a ladder in which each rung divides the next,
 * so a cell of one width lies wholly within a cell of any wider one.
 * A cell holds the rows with `boundary - width < recency <= boundary`, and is keyed by its boundary.
 */
object RecencyCells {
    private const val HOUR = 3_600_000_000L

    private val LADDER = longArrayOf(HOUR, 6 * HOUR, 24 * HOUR)

    private const val MIN_PAGES_PER_CELL = 4

    /**
     * @return the boundary of the cell of [width] microseconds holding [recency]: the multiple of [width] at or above it
     */
    @JvmStatic
    fun boundary(recency: RecencyMicros, width: Long): RecencyMicros = Math.ceilDiv(recency, width) * width

    private fun cellCount(recency: RecencyRange, width: Long) =
        boundary(recency.max, width) / width - boundary(recency.min, width) / width + 1

    /**
     * The width of the cells a file branches into, or null where it doesn't branch on recency at all:
     * the narrowest rung whose cells average at least [MIN_PAGES_PER_CELL] pages of the file's rows,
     * over the cells that [recency] covers.
     *
     * @param rowCount the rows in the file
     * @param recency the range of the file's `_recency`
     * @param pageSize the most rows a page holds
     * @return the width in microseconds; null where no rung's cells average that many pages
     */
    @JvmStatic
    fun width(rowCount: Long, recency: RecencyRange, pageSize: Int): Long? =
        LADDER.firstOrNull { rowCount >= MIN_PAGES_PER_CELL.toLong() * pageSize * cellCount(recency, it) }
}
