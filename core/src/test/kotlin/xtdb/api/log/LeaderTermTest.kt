package xtdb.api.log

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LeaderTermTest {

    @Test
    fun `epoch 0 leaves the election counter unchanged`() {
        val term = LeaderTerm.of(0, 9)

        assertEquals(9, term)
        assertEquals(0, LeaderTerm.epochOf(term))
        assertEquals(9, LeaderTerm.electionOf(term))
    }

    @Test
    fun `round-trips a non-zero epoch`() {
        val term = LeaderTerm.of(3, 9)

        assertEquals(3, LeaderTerm.epochOf(term))
        assertEquals(9, LeaderTerm.electionOf(term))
    }

    @Test
    fun `the epoch dominates the election counter`() {
        // the whole point: a reset election counter still outranks the terms it restarted below
        assertTrue(LeaderTerm.of(1, 1) > LeaderTerm.of(0, (1L shl 48) - 1))
        assertTrue(LeaderTerm.of(0, 9) > LeaderTerm.of(0, 8))
        assertTrue(LeaderTerm.of(0, 1) > LeaderTerm.NONE)
    }

    @Test
    fun `every representable term stays positive, so the ordering never inverts`() {
        val maxTerm = LeaderTerm.of((1 shl 15) - 1, (1L shl 48) - 1)

        assertTrue(maxTerm > 0, "the epoch must not reach the sign bit")
        assertTrue(maxTerm > LeaderTerm.of(0, 1))
    }

    @Test
    fun `rejects values that would corrupt the ordering`() {
        assertThrows<IllegalArgumentException> { LeaderTerm.of(-1, 1) }
        // the sign bit — a negative term would order below every other
        assertThrows<IllegalArgumentException> { LeaderTerm.of(1 shl 15, 1) }
        assertThrows<IllegalArgumentException> { LeaderTerm.of(0, 1L shl 48) }
    }
}
