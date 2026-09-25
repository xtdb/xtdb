package xtdb.postgres

import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ReconnectPolicyTest {

    private val noJitter = object : Random() {
        override fun nextBits(bitCount: Int) = 0
    }

    private val fullJitter = object : Random() {
        override fun nextBits(bitCount: Int) = -1 ushr (32 - bitCount)
    }

    private fun policy(random: Random) = ReconnectPolicy(initialDelay = 100.milliseconds, maxDelay = 1.seconds, jitter = 0.5, random = random)

    @Test
    fun `no failures owes no wait`() {
        assertEquals(Duration.ZERO, policy(fullJitter).delayAfter(0))
    }

    @Test
    fun `the wait doubles with each failure until it reaches the cap`() {
        assertEquals(
            listOf(100, 200, 400, 800, 1000, 1000).map { it.milliseconds },
            (1..6).map { policy(noJitter).delayAfter(it) },
        )
    }

    @Test
    fun `jitter takes off up to its fraction of the wait`() {
        val delay = policy(fullJitter).delayAfter(2)
        assertTrue(delay >= 100.milliseconds && delay < 110.milliseconds, "expected just over 200ms - 50%, got $delay")
    }

    @Test
    fun `a policy needs a positive initial delay no greater than a finite cap`() {
        assertFailsWith<Incorrect> { ReconnectPolicy(initialDelay = Duration.ZERO) }
        assertFailsWith<Incorrect> { ReconnectPolicy(initialDelay = 2.seconds, maxDelay = 1.seconds) }
        assertFailsWith<Incorrect> { ReconnectPolicy(maxDelay = Duration.INFINITE) }
    }

    @Test
    fun `jitter still spreads the wait once it has reached the cap`() {
        assertNotEquals(policy(noJitter).delayAfter(10), policy(fullJitter).delayAfter(10))
    }

    @Test
    fun `any number of failures waits the cap`() {
        assertEquals(1.seconds, policy(noJitter).delayAfter(10_000))
        assertEquals(1.seconds, policy(noJitter).delayAfter(Int.MAX_VALUE))
    }
}
