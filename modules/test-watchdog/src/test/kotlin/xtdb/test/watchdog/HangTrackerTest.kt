package xtdb.test.watchdog

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class HangTrackerTest {

    private val timeout = TimeUnit.SECONDS.toNanos(360)
    private val tracker = HangTracker(timeout)

    @Test
    fun `quiet before the threshold is not overdue`() {
        tracker.planStarted(now = 0)
        tracker.event("started: t1", now = 10)
        assertNull(tracker.overdue(10 + timeout - 1))
    }

    @Test
    fun `reports once at the threshold, again at double, then goes quiet`() {
        tracker.planStarted(now = 0)
        tracker.event("started: t1", now = 0)

        val first = tracker.overdue(timeout)
        assertEquals("started: t1" to 1, first?.let { it.lastEvent to it.report })

        assertNull(tracker.overdue(timeout + 1), "no re-report before the next threshold")

        val second = tracker.overdue(timeout * 2)
        assertEquals(2, second?.report)

        assertNull(tracker.overdue(timeout * 10), "capped at two reports")
    }

    @Test
    fun `any event resets the quiet period and the report count`() {
        tracker.planStarted(now = 0)
        tracker.event("started: t1", now = 0)
        assertEquals(1, tracker.overdue(timeout)?.report)

        tracker.event("finished: t1", now = timeout + 1)
        assertNull(tracker.overdue(timeout * 2))
        assertEquals(1, tracker.overdue(timeout * 2 + 2)?.report)
    }

    @Test
    fun `inactive plan is never overdue`() {
        tracker.planStarted(now = 0)
        tracker.planFinished()
        assertNull(tracker.overdue(timeout * 5))
    }

    @Test
    fun `not overdue before the plan starts`() {
        assertNull(tracker.overdue(timeout * 5))
    }
}
