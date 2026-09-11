package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.indexer.TermFence.Admission.ADMITTED
import xtdb.indexer.TermFence.Admission.CONFERRING
import xtdb.indexer.TermFence.Admission.FENCED

class TermFenceTest {

    @Test
    fun `permits gives the same verdict without folding the term in`() {
        val fence = TermFence(5)

        assertTrue(fence.permits(7))
        assertEquals(5L, fence.highestSeen, "a permitted term teaches the fence nothing")

        assertTrue(fence.permits(5))
        assertFalse(fence.permits(4))
        assertEquals(5L, fence.highestSeen)
    }

    @Test
    fun `a term above every one before it confers, and raises the fence to it`() {
        val fence = TermFence(5)

        assertEquals(ADMITTED, fence.admit(5), "the highest seen is not fenced by itself, and confers nothing")
        assertEquals(5L, fence.highestSeen)

        assertEquals(CONFERRING, fence.admit(7))
        assertEquals(7L, fence.highestSeen)
    }

    @Test
    fun `fences a term below the highest seen, and does not lower it`() {
        val fence = TermFence(7)

        assertEquals(FENCED, fence.admit(6))
        assertEquals(7L, fence.highestSeen, "a fenced record teaches the fence nothing")
    }

    @Test
    fun `term zero is ordered like any other, so a real term fences it`() {
        val fence = TermFence(0)

        assertEquals(ADMITTED, fence.admit(0), "a record written before terms existed has nothing above it yet")
        assertEquals(CONFERRING, fence.admit(1))

        assertEquals(FENCED, fence.admit(0))
    }
}
