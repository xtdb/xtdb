package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Incorrect
import xtdb.api.log.ElectionConfig
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import kotlin.random.Random

class ElectionTest {

    /** A clock that only moves when the test moves it — the simulations have no virtual clock to borrow. */
    private class TestClock(private var at: Instant = Instant.parse("2026-01-01T00:00:00Z")) : InstantSource {
        override fun instant() = at
        fun advance(d: Duration) {
            at = at.plus(d)
        }
    }

    private val config = ElectionConfig(
        electionTimeoutMin = Duration.ofSeconds(6),
        electionTimeoutMax = Duration.ofSeconds(12),
        claimTimeout = Duration.ofSeconds(30),
    )

    private fun stopwatch(clock: TestClock, seed: Int = 0) =
        QuietStopwatch(config, clock, Random(seed))

    @Test
    fun `a fresh participant does not claim on its first read`() {
        val clock = TestClock()
        val sw = stopwatch(clock)

        sw.onEmptyRead()

        assertFalse(sw.quietLongEnough, "a cold-started node claiming immediately is every node claiming at once")
    }

    @Test
    fun `quiet is observed by reading, so time alone advances nothing`() {
        val clock = TestClock()
        val sw = stopwatch(clock)

        clock.advance(Duration.ofMinutes(5))

        assertEquals(Duration.ZERO, sw.quietFor, "no read has happened, so nothing has been observed")
        assertFalse(sw.quietLongEnough)
        assertFalse(sw.claimOverdue)

        sw.onEmptyRead()

        assertTrue(sw.quietLongEnough, "the read is what makes the silence observable")
    }

    @Test
    fun `a record read defers the election however long the log had been quiet`() {
        val clock = TestClock()
        val sw = stopwatch(clock)

        clock.advance(Duration.ofMinutes(5))
        sw.onEmptyRead()
        assertTrue(sw.quietLongEnough)

        sw.onRecord()

        assertEquals(Duration.ZERO, sw.quietFor)
        assertFalse(sw.quietLongEnough, "any record at all is an assertion that somebody is leading")
    }

    @Test
    fun `time spent processing a record is not quiet`() {
        val clock = TestClock()
        val sw = stopwatch(clock)

        // The record arrived here, and took longer to process than any election timeout.
        clock.advance(Duration.ofMinutes(1))
        sw.onRecord()

        sw.onEmptyRead()

        assertEquals(
            Duration.ZERO, sw.quietFor,
            "stamping the record on arrival instead would have this follower claim for being slow"
        )
        assertFalse(sw.quietLongEnough)
    }

    @Test
    fun `the claim timeout is a longer wait than any election timeout`() {
        val clock = TestClock()
        val sw = stopwatch(clock)

        clock.advance(Duration.ofSeconds(13))
        sw.onEmptyRead()

        assertTrue(sw.quietLongEnough, "past the election-timeout maximum")
        assertFalse(sw.claimOverdue, "but nowhere near giving up on a claim")

        clock.advance(Duration.ofSeconds(20))
        sw.onEmptyRead()

        assertTrue(sw.claimOverdue)
    }

    @Test
    fun `an abandoned claim widens the wait, and a verdict narrows it back`() {
        val clock = TestClock()
        val sw = stopwatch(clock)

        sw.backOff()
        assertEquals(1, sw.abandonedClaims)

        // One abandonment scales the range by 1 + 2*1 = 3, so the floor is 18s rather than 6s.
        clock.advance(Duration.ofSeconds(17))
        sw.onEmptyRead()
        assertFalse(sw.quietLongEnough, "backed off, so the previous floor is no longer enough")

        sw.backOff()
        assertEquals(2, sw.abandonedClaims)

        sw.restartWait()
        assertEquals(0, sw.abandonedClaims, "a claim that reached a verdict is evidence reads are arriving")

        clock.advance(Duration.ofSeconds(13))
        sw.onEmptyRead()
        assertTrue(sw.quietLongEnough, "back to the unscaled range")
    }

    @Test
    fun `the drawn timeout stays inside the configured range`() {
        // Every draw lands in [6s, 12s], so 5s of quiet is never enough and 12s always is.
        for (seed in 0 until 200) {
            val shortClock = TestClock()
            val short = stopwatch(shortClock, seed)
            shortClock.advance(Duration.ofSeconds(5))
            short.onEmptyRead()
            assertFalse(short.quietLongEnough, "seed $seed drew below the configured minimum")

            val longClock = TestClock()
            val long = stopwatch(longClock, seed)
            longClock.advance(Duration.ofSeconds(12))
            long.onEmptyRead()
            assertTrue(long.quietLongEnough, "seed $seed drew above the configured maximum")
        }
    }

    @Test
    fun `a claim timeout at or below the election maximum is refused`() {
        assertThrows<Incorrect> {
            ElectionConfig(
                electionTimeoutMin = Duration.ofSeconds(6),
                electionTimeoutMax = Duration.ofSeconds(12),
                claimTimeout = Duration.ofSeconds(12),
            )
        }
    }

    @Test
    fun `an inverted election timeout range is refused`() {
        assertThrows<Incorrect> {
            ElectionConfig(
                electionTimeoutMin = Duration.ofSeconds(12),
                electionTimeoutMax = Duration.ofSeconds(6),
                claimTimeout = Duration.ofSeconds(30),
            )
        }
    }
}
