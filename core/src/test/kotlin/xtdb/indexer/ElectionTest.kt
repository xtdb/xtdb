package xtdb.indexer

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
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

    private fun driver(clock: TestClock, seed: Int = 0) = QuietDriver(config, clock, Random(seed))

    /** What the participant's loop would take off the driver's arm right now, or null if nothing is armed. */
    private fun ElectionDriver.fired(): Quiet? {
        val arm = onTimeout
        return runBlocking { withTimeoutOrNull(1) { select { arm { it } } } }
    }

    @Test
    fun `a fresh participant does not claim on its first read`() {
        val clock = TestClock()
        val driver = driver(clock).apply { await(Quiet.ELECTION) }

        driver.onEmptyRead()

        assertNull(driver.fired(), "a cold-started node claiming immediately is every node claiming at once")
    }

    @Test
    fun `quiet is observed by reading, so time alone advances nothing`() {
        val clock = TestClock()
        val driver = driver(clock).apply { await(Quiet.ELECTION) }

        clock.advance(Duration.ofMinutes(5))

        assertNull(driver.fired(), "no read has happened, so nothing has been observed")

        driver.onEmptyRead()

        assertEquals(Quiet.ELECTION, driver.fired(), "the read is what makes the silence observable")
    }

    @Test
    fun `a record read defers the election however long the log had been quiet`() {
        val clock = TestClock()
        val driver = driver(clock).apply { await(Quiet.ELECTION) }

        clock.advance(Duration.ofMinutes(5))
        driver.onRecord()
        driver.onEmptyRead()

        assertNull(driver.fired(), "any record at all is an assertion that somebody is leading")
    }

    @Test
    fun `a record withdraws a timeout the loop has not yet answered`() {
        val clock = TestClock()
        val driver = driver(clock).apply { await(Quiet.ELECTION) }

        clock.advance(Duration.ofMinutes(5))
        driver.onEmptyRead()
        driver.onRecord()

        assertNull(driver.fired(), "acting on it would claim against a leader we have just heard from")
    }

    @Test
    fun `time spent processing a record is not quiet`() {
        val clock = TestClock()
        val driver = driver(clock).apply { await(Quiet.ELECTION) }

        // The record arrived here, and took longer to process than any election timeout.
        clock.advance(Duration.ofMinutes(1))
        driver.onRecord()

        driver.onEmptyRead()

        assertNull(driver.fired(), "stamping the record on arrival instead would have this follower claim for being slow")
    }

    @Test
    fun `waiting on a claim outlasts any election timeout`() {
        val clock = TestClock()
        val driver = driver(clock).apply { await(Quiet.CLAIM_VERDICT) }

        clock.advance(Duration.ofSeconds(13))
        driver.onEmptyRead()

        assertNull(driver.fired(), "past the election-timeout maximum, but nowhere near giving up on a claim")

        clock.advance(Duration.ofSeconds(20))
        driver.onEmptyRead()

        assertEquals(Quiet.CLAIM_VERDICT, driver.fired())
    }

    @Test
    fun `an abandoned claim widens the wait, and a verdict narrows it back`() {
        val clock = TestClock()
        val driver = driver(clock)

        driver.backOff()

        // One abandonment scales the range by 1 + 2*1 = 3, so the floor is 18s rather than 6s.
        clock.advance(Duration.ofSeconds(17))
        driver.onEmptyRead()
        assertNull(driver.fired(), "backed off, so the previous floor is no longer enough")

        driver.await(Quiet.ELECTION)

        clock.advance(Duration.ofSeconds(13))
        driver.onEmptyRead()
        assertEquals(Quiet.ELECTION, driver.fired(), "a claim that reached a verdict is evidence reads are arriving")
    }

    @Test
    fun `the drawn timeout stays inside the configured range`() {
        // Every draw lands in [6s, 12s], so 5s of quiet is never enough and 12s always is.
        for (seed in 0 until 200) {
            val shortClock = TestClock()
            val short = driver(shortClock, seed).apply { await(Quiet.ELECTION) }
            shortClock.advance(Duration.ofSeconds(5))
            short.onEmptyRead()
            assertNull(short.fired(), "seed $seed drew below the configured minimum")

            val longClock = TestClock()
            val long = driver(longClock, seed).apply { await(Quiet.ELECTION) }
            longClock.advance(Duration.ofSeconds(12))
            long.onEmptyRead()
            assertEquals(Quiet.ELECTION, long.fired(), "seed $seed drew above the configured maximum")
        }
    }

    @Test
    fun `an idle driver concludes nothing however long the log stays quiet`() {
        val clock = TestClock()
        val driver = driver(clock).apply { idle() }

        clock.advance(Duration.ofHours(1))
        driver.onEmptyRead()

        assertNull(driver.fired(), "a node that may not lead is entitled to conclude nothing from silence")
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
