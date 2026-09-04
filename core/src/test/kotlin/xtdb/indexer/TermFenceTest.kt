package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.log.LeaderTerm
import xtdb.indexer.TermFence.Admission.ADMITTED
import xtdb.indexer.TermFence.Admission.CONFERRING
import xtdb.indexer.TermFence.Admission.FENCED

class TermFenceTest {

    @Test
    fun `a term equal to the highest seen is admitted, and confers nothing`() {
        val fence = TermFence(5L)

        assertEquals(ADMITTED, fence.admit(5L), "the highest seen is not fenced by itself")
        assertEquals(5L, fence.highestSeen)
    }

    @Test
    fun `a term above the highest seen confers, and raises the fence to it`() {
        val fence = TermFence(5L)

        assertEquals(CONFERRING, fence.admit(7L))
        assertEquals(7L, fence.highestSeen)
    }

    @Test
    fun `a second record at a conferred term confers nothing further`() {
        val fence = TermFence(5L)

        assertEquals(CONFERRING, fence.admit(7L))
        assertEquals(ADMITTED, fence.admit(7L), "leadership is conferred once, by the first record at a term")
    }

    @Test
    fun `fences a term below the highest seen, and does not lower it`() {
        val fence = TermFence(7L)

        assertEquals(FENCED, fence.admit(6L))
        assertEquals(7L, fence.highestSeen, "a fenced record teaches the fence nothing")
    }

    @Test
    fun `the unset term is never fenced and never counts`() {
        val fence = TermFence(7L)

        assertEquals(ADMITTED, fence.admit(LeaderTerm.NONE), "a record from before terms existed is still applied")
        assertEquals(7L, fence.highestSeen)

        val fresh = TermFence(LeaderTerm.NONE)
        assertEquals(ADMITTED, fresh.admit(LeaderTerm.NONE), "and never reads as an election")
        assertEquals(LeaderTerm.NONE, fresh.highestSeen)
    }

    @Test
    fun `the first term on a fresh log confers`() {
        val fence = TermFence(LeaderTerm.NONE)

        assertEquals(CONFERRING, fence.admit(1L))
    }

}
