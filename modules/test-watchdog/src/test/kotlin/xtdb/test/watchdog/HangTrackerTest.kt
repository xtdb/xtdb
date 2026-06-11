package xtdb.test.watchdog

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class HangTrackerTest {

    private val timeout = TimeUnit.SECONDS.toNanos(360)
    private val tracker = HangTracker(timeout)

    @Test
    fun `not overdue before the threshold`() {
        tracker.started("t1", "test one", now = 0)
        assertTrue(tracker.overdue(timeout - 1).isEmpty())
    }

    @Test
    fun `reports once at the threshold, again at double, then goes quiet`() {
        tracker.started("t1", "test one", now = 0)

        val first = tracker.overdue(timeout)
        assertEquals(listOf("test one" to 1), first.map { it.displayName to it.report })

        assertTrue(tracker.overdue(timeout + 1).isEmpty(), "no re-report before the next threshold")

        val second = tracker.overdue(timeout * 2)
        assertEquals(listOf("test one" to 2), second.map { it.displayName to it.report })

        assertTrue(tracker.overdue(timeout * 10).isEmpty(), "capped at two reports")
    }

    @Test
    fun `finished tests are never reported`() {
        tracker.started("t1", "test one", now = 0)
        tracker.finished("t1")
        assertTrue(tracker.overdue(timeout * 2).isEmpty())
    }

    @Test
    fun `only the overdue test among several is reported`() {
        tracker.started("t1", "slow", now = 0)
        tracker.started("t2", "fresh", now = timeout)
        assertEquals(listOf("slow"), tracker.overdue(timeout).map { it.displayName })
    }
}
