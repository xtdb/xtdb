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
    fun `the unset term is never fenced and never counts`() {
        val fence = TermFence(dbName, LeaderTerm.of(0, 7))

        assertTrue(fence.admit(LeaderTerm.NONE), "a record from before terms existed is still applied")
        assertEquals(LeaderTerm.of(0, 7), fence.highestSeen)

        val fresh = TermFence(dbName, LeaderTerm.NONE)
        assertTrue(fresh.admit(LeaderTerm.NONE))
        assertEquals(LeaderTerm.NONE, fresh.highestSeen)
    }

    @Test
    fun `a higher epoch outranks any election within a lower one`() {
        val fence = TermFence(dbName, LeaderTerm.of(0, 9))

        assertTrue(fence.admit(LeaderTerm.of(1, 1)))
        assertFalse(fence.admit(LeaderTerm.of(0, 9)), "the earlier epoch is now behind, whatever its election")
    }
}
