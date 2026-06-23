package xtdb.diagnostics

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import java.time.Duration
import java.util.ServiceLoader

/**
 * Bounds a teardown that a cancellation-immune wait would otherwise hang forever (xtdb#5711). A probe
 * — provided by `xtdb-test-watchdog` via ServiceLoader — supplies the timeout and dumps the wedged
 * coroutines on a stall. Production ships no impl, so teardown runs unbounded there.
 */
interface TeardownStallProbe {
    val timeout: Duration
    fun onStall(reason: String)
}

object TeardownStall {
    private val probe: TeardownStallProbe? = ServiceLoader.load(TeardownStallProbe::class.java).firstOrNull()

    /** Runs [teardown] — bounded + dumped-on-stall under test (probe present), unbounded otherwise; returns true iff it stalled. */
    fun runBounded(reason: String, teardown: suspend () -> Unit): Boolean = runBlocking {
        val p = probe
        if (p == null) {
            teardown()
            false
        } else try {
            withTimeout(p.timeout) { teardown() }
            false
        } catch (_: TimeoutCancellationException) {
            p.onStall("$reason exceeded ${p.timeout.toSeconds()}s")
            true
        }
    }
}
