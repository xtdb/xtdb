package xtdb.test.watchdog

/**
 * Tracks test-plan liveness and reports when it has gone quiet for too long.
 *
 * The trigger is deliberately "no test lifecycle event for [timeoutNanos]" rather than
 * per-test elapsed time: it needs only one timestamp, and it also catches hangs *between*
 * tests (fixtures, `@BeforeAll`/`@AfterAll`, engine teardown) — where this bug class lives —
 * which per-test tracking misses.
 *
 * Pure bookkeeping with caller-supplied nanos, so the threshold logic is unit-testable with
 * a fake clock. [event] is called from test threads; [overdue] from the single scanner
 * thread.
 */
class HangTracker(
    private val timeoutNanos: Long,
    private val maxReports: Int = 2,
) {
    @Volatile
    private var active = false

    @Volatile
    private var lastEventAt = 0L

    @Volatile
    private var lastEvent = ""

    @Volatile
    private var reports = 0

    fun planStarted(now: Long) {
        lastEvent = "test plan started"
        lastEventAt = now
        reports = 0
        active = true
    }

    fun planFinished() {
        active = false
    }

    fun event(description: String, now: Long) {
        lastEvent = description
        lastEventAt = now
        reports = 0
    }

    data class Overdue(val quietNanos: Long, val lastEvent: String, val report: Int)

    /**
     * Non-null when the quiet period has crossed `timeout * (reports + 1)` — each crossing
     * reports once, up to [maxReports], so a hang produces two dumps (movement between them
     * distinguishes stuck from slow) and then goes quiet rather than flooding the log.
     */
    fun overdue(now: Long): Overdue? {
        if (!active) return null
        val quiet = now - lastEventAt
        return if (reports < maxReports && quiet >= timeoutNanos * (reports + 1))
            Overdue(quiet, lastEvent, ++reports)
        else null
    }
}
