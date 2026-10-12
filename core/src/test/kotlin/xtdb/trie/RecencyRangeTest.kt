package xtdb.trie

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RecencyRangeTest {

    @Test
    fun `a node splits four ways while its parts will branch again, otherwise into about a page each`() {
        assertEquals(
            listOf(2, 2, 3, 3, 4, 4, 4),
            listOf(1025, 2048, 2049, 3072, 3073, 4096, 100_000).map { RecencyRange.splitCount(it, 1024) }
        )
    }

    @Test
    fun `equal-width parts cover the range, oldest first`() {
        val range = RecencyRange(100, 199)

        assertEquals(
            listOf(0, 0, 1, 1, 2, 2, 3, 3),
            listOf(100, 124, 125, 149, 150, 174, 175, 199).map { range.partIdx(it.toLong(), 4) }
        )

        assertEquals(listOf(0, 1), listOf(100L, 199L).map { range.partIdx(it, 2) })
    }

    @Test
    fun `every recency in a week-wide range falls into one of its parts`() {
        val weekMicros = 7L * 24 * 60 * 60 * 1_000_000
        val range = RecencyRange(1_577_836_800_000_000, 1_577_836_800_000_000 + weekMicros)

        for (k in 2..4) {
            val parts = (0..1000).map { range.partIdx(range.min + weekMicros * it / 1000, k) }
            assertEquals(0, parts.first())
            assertEquals(k - 1, parts.last())
            assertTrue(parts.zipWithNext().all { (a, b) -> a <= b && b - a <= 1 }, "parts ascend without gaps")
        }
    }

    @Test
    fun `ranges union`() {
        assertEquals(RecencyRange(1, 9), RecencyRange(3, 9) + RecencyRange(1, 4))
        assertEquals(RecencyRange(3, 9), RecencyRange.EMPTY + RecencyRange(3, 9))
        assertEquals(false, RecencyRange(5, 5).splittable)
    }
}
