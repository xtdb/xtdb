package xtdb.test.watchdog

import xtdb.diagnostics.TeardownStallProbe

/**
 * Routes a teardown stall (e.g. `DatabaseCatalog.close` abandoning a wedged job tree, xtdb#5711) into the
 * watchdog's coroutine + thread dump. The bounded escape stops the test hanging, so the test-plan
 * scanner never fires — this captures the stalled state at the moment of the stall instead.
 *
 * Discovered via ServiceLoader; this module rides every project's testRuntimeOnly classpath, so it
 * wires up with no per-project config. Absent in production, where `TeardownStall` is a no-op.
 */
class WatchdogStallProbe : TeardownStallProbe {
    override fun onStall(reason: String) = HangWatchdog.dumpNow(reason)
}
