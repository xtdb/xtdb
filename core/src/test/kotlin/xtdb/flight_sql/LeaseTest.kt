package xtdb.flight_sql

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import xtdb.flight_sql.Leases.Lookup

class LeaseTest {

    private class TestClock(@Volatile var now: Instant = Instant.parse("2020-01-01T00:00:00Z")) : InstantSource {
        override fun instant(): Instant = now
        fun advance(d: Duration) { now = now.plus(d) }
    }

    private class Held(override val lease: Lease) : Leased {
        val closes = AtomicInteger()
        override fun close() { closes.incrementAndGet() }
    }

    private val clock = TestClock()
    private val leases = Leases<String, Held>(clock)

    private fun held(idle: Duration = Duration.ofMinutes(10), first: Duration = idle) =
        Held(Lease(clock, idle, first))

    @Test
    fun `each lookup renews the lease for its idle timeout`() {
        val h = leases.add("h", held())

        clock.advance(Duration.ofMinutes(9))
        assertEquals(Lookup.Found(h), leases.lookup("h"))

        clock.advance(Duration.ofMinutes(9))
        assertEquals(Lookup.Found(h), leases.lookup("h"))

        clock.advance(Duration.ofMinutes(10))
        assertEquals(Lookup.Expired(Duration.ofMinutes(10)), leases.lookup("h"))
        assertEquals(1, h.closes.get(), "the lookup that finds it lapsed closes it")
        assertEquals(Lookup.Expired(Duration.ofMinutes(10)), leases.lookup("h"))
        assertEquals(1, h.closes.get())
    }

    @Test
    fun `a lease is live for its first limit until it is first renewed`() {
        val unclaimed = leases.add("unclaimed", held(first = Duration.ofMinutes(1)))
        val claimed = leases.add("claimed", held(first = Duration.ofMinutes(1)))

        clock.advance(Duration.ofSeconds(30))
        assertEquals(Lookup.Found(claimed), leases.lookup("claimed"))

        clock.advance(Duration.ofMinutes(5))
        assertEquals(Lookup.Expired(Duration.ofMinutes(1)), leases.lookup("unclaimed"))
        assertEquals(1, unclaimed.closes.get())
        assertEquals(Lookup.Found(claimed), leases.lookup("claimed"))
    }

    @Test
    fun `a lease in use doesn't lapse, and its limit runs again from its release`() {
        val h = leases.add("h", held())
        assertTrue(h.lease.acquire())

        clock.advance(Duration.ofHours(1))
        leases.sweep()
        assertEquals(0, h.closes.get())

        assertTrue(h.lease.release())
        clock.advance(Duration.ofMinutes(9))
        assertEquals(Lookup.Found(h), leases.lookup("h"))

        clock.advance(Duration.ofMinutes(10))
        assertEquals(Lookup.Expired(Duration.ofMinutes(10)), leases.lookup("h"))
    }

    @Test
    fun `holding a lease keeps its first limit`() {
        val h = leases.add("h", held(first = Duration.ofMinutes(1)))
        h.lease.acquire()
        clock.advance(Duration.ofMinutes(5))
        h.lease.release()

        clock.advance(Duration.ofMinutes(2))
        assertEquals(Lookup.Expired(Duration.ofMinutes(1)), leases.lookup("h"))
    }

    @Test
    fun `acquiring by key moves an unclaimed lease onto its idle timeout, and holds it until its release`() {
        val h = leases.add("h", held(first = Duration.ofMinutes(1)))

        clock.advance(Duration.ofSeconds(30))
        assertEquals(Lookup.Found(h), leases.acquire("h"))

        clock.advance(Duration.ofHours(1))
        leases.sweep()
        assertEquals(0, h.closes.get())

        assertTrue(h.lease.release())
        clock.advance(Duration.ofMinutes(9))
        assertEquals(Lookup.Found(h), leases.lookup("h"))
    }

    @Test
    fun `take hands the value to exactly one caller and forgets the handle`() {
        val h = leases.add("h", held())

        assertEquals(Lookup.Found(h), leases.take("h"))
        assertEquals(Lookup.Unknown, leases.take("h"))
        assertEquals(Lookup.Unknown, leases.lookup("h"))

        assertEquals(0, h.closes.get(), "closing a taken value is the taker's")
        clock.advance(Duration.ofDays(1))
        leases.sweep()
        assertEquals(0, h.closes.get())
    }

    @Test
    fun `a lapsed value can't be taken`() {
        val h = leases.add("h", held())
        clock.advance(Duration.ofMinutes(10))

        assertEquals(Lookup.Expired(Duration.ofMinutes(10)), leases.take("h"))
        assertEquals(1, h.closes.get())
    }

    @Test
    fun `sweep closes lapsed values and forgets each once it has been expired for as long as its limit`() {
        val lapsed = leases.add("lapsed", held(idle = Duration.ofMinutes(1)))
        val live = leases.add("live", held())

        clock.advance(Duration.ofMinutes(2))
        leases.sweep()
        assertEquals(1, lapsed.closes.get())
        assertEquals(0, live.closes.get())
        assertEquals(Lookup.Expired(Duration.ofMinutes(1)), leases.lookup("lapsed"))

        clock.advance(Duration.ofMinutes(1))
        leases.sweep()
        assertEquals(Lookup.Unknown, leases.lookup("lapsed"))
        assertEquals(1, lapsed.closes.get())
    }

    @Test
    fun `closing the registry closes what's still held`() {
        val live = leases.add("live", held())
        val taken = leases.add("taken", held())
        leases.take("taken")

        leases.close()

        assertEquals(1, live.closes.get())
        assertEquals(0, taken.closes.get())
    }

    @Test
    fun `a value racing renewal against expiry is closed at most once, and never found after it's closed`() {
        val pool = Executors.newFixedThreadPool(4)
        try {
            repeat(200) { i ->
                val h = leases.add("h$i", held(idle = Duration.ofMillis(1)))
                val foundAfterClose = AtomicBoolean(false)
                val start = CountDownLatch(1)

                val lookups = List(2) {
                    pool.submit {
                        start.await()
                        repeat(50) {
                            val closedBefore = h.closes.get() > 0
                            if (leases.lookup("h$i") is Lookup.Found && closedBefore) foundAfterClose.set(true)
                        }
                    }
                }
                val sweeper = pool.submit {
                    start.await()
                    repeat(50) { leases.sweep() }
                }
                val ticker = pool.submit {
                    start.await()
                    repeat(50) { clock.advance(Duration.ofNanos(500_000)) }
                }

                start.countDown()
                (lookups + sweeper + ticker).forEach { it.get(10, TimeUnit.SECONDS) }

                assertTrue(h.closes.get() <= 1, "closed ${h.closes.get()} times")
                assertFalse(foundAfterClose.get())
            }
        } finally {
            pool.shutdownNow()
        }
    }
}
