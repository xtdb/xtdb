package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.log.LeaderTerm

class TermFenceTest {

    @Test
    fun `admits a term at or above the highest seen, and raises to it`() {
        val fence = TermFence(5)

        assertTrue(fence.admit(5), "the highest seen is not fenced by itself")
        assertEquals(5, fence.highest)

        assertTrue(fence.admit(7))
        assertEquals(7, fence.highest)
    }

    @Test
    fun `fences a term below the highest seen, and does not lower it`() {
        val fence = TermFence(7)

        assertFalse(fence.admit(6))
        assertEquals(7, fence.highest, "a fenced record teaches the fence nothing")
    }

    @Test
    fun `the unset term is never fenced and never counts`() {
        val fence = TermFence(7)

        assertTrue(fence.admit(LeaderTerm.NONE), "a record from before terms existed is still applied")
        assertEquals(7, fence.highest)

        val fresh = TermFence(LeaderTerm.NONE)
        assertTrue(fresh.admit(LeaderTerm.NONE))
        assertEquals(LeaderTerm.NONE, fresh.highest)
    }
}
