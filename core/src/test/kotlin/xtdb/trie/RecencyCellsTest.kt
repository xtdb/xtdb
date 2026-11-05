package xtdb.trie

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class RecencyCellsTest {
    private val hour = 3_600_000_000L
    private val day = 24 * hour

    // 2020-01-06T00:00Z, a Monday
    private val weekStart = 1_578_268_800_000_000L
    private val week = RecencyRange(weekStart + 1, weekStart + 7 * day)

    @Test
    fun `a cell is keyed by its inclusive upper boundary`() {
        assertEquals(
            listOf(weekStart, weekStart + hour, weekStart + hour, weekStart + 2 * hour),
            listOf(weekStart, weekStart + 1, weekStart + hour, weekStart + hour + 1).map { RecencyCells.boundary(it, hour) }
        )
    }

    @Test
    fun `a file takes the narrowest of 1h, 6h and 24h whose cells average four pages of its rows`() {
        assertEquals(
            hour,
            RecencyCells.width(9375L * 168, week, pageSize = 1024),
            "600k rows an hour across 64 shards"
        )

        assertEquals(6 * hour, RecencyCells.width(1_000L * 168, week, pageSize = 1024))
        assertEquals(day, RecencyCells.width(400L * 168, week, pageSize = 1024))
    }

    @Test
    fun `cells average over those the file's recency covers, not over its week`() {
        assertEquals(hour, RecencyCells.width(4096, RecencyRange(weekStart, weekStart), pageSize = 1024), "one instant, one cell")

        assertEquals(hour, RecencyCells.width(8192, RecencyRange(weekStart + 1, weekStart + 2 * hour), pageSize = 1024))

        assertEquals(
            6 * hour,
            RecencyCells.width(8192, RecencyRange(weekStart, weekStart + 2 * hour), pageSize = 1024),
            "two hours that touch three 1h cells"
        )
    }

    @Test
    fun `a file has no cells where no rung's cells average four pages of its rows`() {
        assertNull(RecencyCells.width(150L * 168, week, pageSize = 1024))
        assertNull(RecencyCells.width(4095, RecencyRange(weekStart, weekStart), pageSize = 1024))
    }
}
