package xtdb.test.watchdog

import xtdb.diagnostics.TeardownStallProbe
import java.time.Duration

/**
 * The test-side `TeardownStallProbe` (ServiceLoader-discovered): supplies the bound and routes the
 * stall dump to the watchdog. Present on any test runtime, absent in production.
 */
class WatchdogStallProbe : TeardownStallProbe {
    override val timeout: Duration get() = HangWatchdog.teardownTimeout
    override fun onStall(reason: String) = HangWatchdog.dumpNow(reason)
}
