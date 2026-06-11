package xtdb.test.watchdog

import java.util.concurrent.ConcurrentHashMap

/**
 * Tracks in-flight tests and reports the ones that have exceeded the hang threshold.
 *
 * Pure bookkeeping — the listener owns scheduling and dumping — so the threshold logic is
 * unit-testable with a fake clock. Times are raw nanos from the caller's clock.
 */
class HangTracker(
    private val timeoutNanos: Long,
    private val maxReports: Int = 2,
) {
    private class Entry(val displayName: String, val startedAt: Long) {
        var reports = 0
    }

    private val running = ConcurrentHashMap<String, Entry>()

    fun started(id: String, displayName: String, now: Long) {
        running[id] = Entry(displayName, now)
    }

    fun finished(id: String) {
        running.remove(id)
    }

    data class Overdue(val displayName: String, val elapsedNanos: Long, val report: Int)

    /**
     * Tests whose run time has crossed `timeout * (reports + 1)` — each crossing reports once,
     * up to [maxReports], so a hung test produces two dumps (movement between them distinguishes
     * "stuck" from "slow") and then goes quiet rather than flooding the log.
     */
    fun overdue(now: Long): List<Overdue> =
        running.values.mapNotNull { e ->
            val elapsed = now - e.startedAt
            if (e.reports < maxReports && elapsed >= timeoutNanos * (e.reports + 1))
                Overdue(e.displayName, elapsed, ++e.reports)
            else null
        }
}
