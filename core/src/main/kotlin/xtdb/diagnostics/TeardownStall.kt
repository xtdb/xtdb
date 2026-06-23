package xtdb.diagnostics

import java.util.ServiceLoader

/**
 * Fired when a bounded teardown wait gives up — e.g. `DatabaseCatalog.close` abandoning a job tree
 * that didn't respond to cancellation (xtdb#5711). The point is to capture the stalled coroutine
 * state *before* the escape proceeds, since the escape turns the hang into a fast return and so the
 * test-plan hang-watchdog (which only fires on a stalled test) never gets the chance to dump it.
 *
 * No-op unless an impl is on the classpath: the `xtdb-test-watchdog` module provides one (via
 * ServiceLoader) that dumps coroutines + threads. Production carries no impl.
 */
fun interface TeardownStallProbe {
    fun onStall(reason: String)
}

object TeardownStall {
    private val probe: TeardownStallProbe? = ServiceLoader.load(TeardownStallProbe::class.java).firstOrNull()

    fun onStall(reason: String) {
        probe?.onStall(reason)
    }
}
