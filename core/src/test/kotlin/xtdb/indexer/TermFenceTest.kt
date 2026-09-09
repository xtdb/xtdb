package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.log.LeaderTerm

class TermFenceTest {

    private val dbName = "test-db"

    @Test
    fun `admits a term at or above the highest seen, and raises to it`() {
        val fence = TermFence(dbName, LeaderTerm.of(0, 5))

        assertTrue(fence.admit(LeaderTerm.of(0, 5)), "the highest seen is not fenced by itself")
        assertEquals(LeaderTerm.of(0, 5), fence.highestSeen)

        assertTrue(fence.admit(LeaderTerm.of(0, 7)))
        assertEquals(LeaderTerm.of(0, 7), fence.highestSeen)
    }

    @Test
    fun `fences a term below the highest seen, and does not lower it`() {
        val fence = TermFence(dbName, LeaderTerm.of(0, 7))

        assertFalse(fence.admit(LeaderTerm.of(0, 6)))
        assertEquals(LeaderTerm.of(0, 7), fence.highestSeen, "a fenced record teaches the fence nothing")
    }

    @Test
    fun `term zero is ordered like any other, so a real term fences it`() {
        val fence = TermFence(dbName, 0)

        assertTrue(fence.admit(0), "a record written before terms existed has nothing above it yet")
        assertTrue(fence.admit(LeaderTerm.of(0, 1)))

        assertFalse(fence.admit(0))
    }

    @Test
    fun `a higher epoch outranks any election within a lower one`() {
        val fence = TermFence(dbName, LeaderTerm.of(0, 9))

        assertTrue(fence.admit(LeaderTerm.of(1, 1)))
        assertFalse(fence.admit(LeaderTerm.of(0, 9)), "the earlier epoch is now behind, whatever its election")
    }
}
