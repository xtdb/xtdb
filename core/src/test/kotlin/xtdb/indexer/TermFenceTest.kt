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
        val fence = TermFence(LeaderTerm.of(0, 5))

        assertEquals(ADMITTED, fence.admit(LeaderTerm.of(0, 5)), "the highest seen is not fenced by itself")
        assertEquals(LeaderTerm.of(0, 5), fence.highestSeen)
    }

    @Test
    fun `a term above the highest seen confers, and raises the fence to it`() {
        val fence = TermFence(LeaderTerm.of(0, 5))

        assertEquals(CONFERRING, fence.admit(LeaderTerm.of(0, 7)))
        assertEquals(LeaderTerm.of(0, 7), fence.highestSeen)
    }

    @Test
    fun `a second record at a conferred term confers nothing further`() {
        val fence = TermFence(LeaderTerm.of(0, 5))

        assertEquals(CONFERRING, fence.admit(LeaderTerm.of(0, 7)))
        assertEquals(ADMITTED, fence.admit(LeaderTerm.of(0, 7)), "leadership is conferred once, by the first record at a term")
    }

    @Test
    fun `fences a term below the highest seen, and does not lower it`() {
        val fence = TermFence(LeaderTerm.of(0, 7))

        assertEquals(FENCED, fence.admit(LeaderTerm.of(0, 6)))
        assertEquals(LeaderTerm.of(0, 7), fence.highestSeen, "a fenced record teaches the fence nothing")
    }

    @Test
    fun `the unset term is never fenced and never counts`() {
        val fence = TermFence(LeaderTerm.of(0, 7))

        assertEquals(ADMITTED, fence.admit(LeaderTerm.NONE), "a record from before terms existed is still applied")
        assertEquals(LeaderTerm.of(0, 7), fence.highestSeen)

        val fresh = TermFence(LeaderTerm.NONE)
        assertEquals(ADMITTED, fresh.admit(LeaderTerm.NONE), "and never reads as an election")
        assertEquals(LeaderTerm.NONE, fresh.highestSeen)
    }

    @Test
    fun `the first term on a fresh log confers`() {
        val fence = TermFence(LeaderTerm.NONE)

        assertEquals(CONFERRING, fence.admit(LeaderTerm.of(0, 1)))
    }

    @Test
    fun `a higher epoch outranks any election within a lower one`() {
        val fence = TermFence(LeaderTerm.of(0, 9))

        assertEquals(CONFERRING, fence.admit(LeaderTerm.of(1, 1)))
        assertEquals(FENCED, fence.admit(LeaderTerm.of(0, 9)), "the earlier epoch is now behind, whatever its election")
    }
}
