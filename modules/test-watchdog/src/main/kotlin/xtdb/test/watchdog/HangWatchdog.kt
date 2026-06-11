package xtdb.test.watchdog

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.debug.DebugProbes
import org.junit.platform.engine.TestExecutionResult
import org.junit.platform.launcher.TestExecutionListener
import org.junit.platform.launcher.TestIdentifier
import org.junit.platform.launcher.TestPlan
import java.io.FileDescriptor
import java.io.FileOutputStream
import java.io.PrintStream
import java.lang.management.ManagementFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Dumps coroutines and threads when the test plan goes quiet for too long, so a hung CI job
 * names its stuck coroutine in the log instead of dying silently at the action timeout — see
 * xtdb#5711.
 *
 * Diagnostic only: it never fails or interrupts anything; a slow-but-progressing run that
 * trips it costs log noise, not a failure. Registered via ServiceLoader
 * (`META-INF/services/org.junit.platform.launcher.TestExecutionListener`); this module rides
 * every project's testRuntimeOnly classpath, so every `Test` task gets it with no per-project
 * config.
 *
 * Coroutine probes are expected to be pre-installed by running the JVM with
 * `-javaagent:kotlinx-coroutines-debug.jar` (the root build adds this to Test tasks) —
 * static premain installation needs only `java.instrument`, unlike dynamic self-attach,
 * which would force `jdk.attach` into the custom JRE that production Docker images share.
 * Without the agent, the watchdog degrades to thread dumps.
 *
 * System properties: `xtdb.test.hang-watchdog.enabled` (default `true`),
 * `xtdb.test.hang-watchdog.timeout-seconds` (default 240 — roughly 2× the longest
 * legitimate quiet period observed on CI runners, and small enough that a hang starting
 * late in a 15-minute job still dumps before the job timeout).
 *
 * The instance is a thin forwarder: all state lives in [Companion], so it's one tracker and
 * one scanner per JVM however many listener instances JUnit/Gradle construct (the companion
 * is a single per-class object, not per-instance). Every instance feeds the one tracker the
 * one scanner reads.
 */
class HangWatchdog : TestExecutionListener {

    override fun testPlanExecutionStarted(testPlan: TestPlan) {
        if (!enabled) return
        tracker.planStarted(System.nanoTime())
        ensureScanner()
    }

    override fun testPlanExecutionFinished(testPlan: TestPlan) {
        if (enabled) tracker.planFinished()
    }

    override fun executionStarted(id: TestIdentifier) {
        if (enabled) tracker.event("started: ${id.displayName}", System.nanoTime())
    }

    override fun executionFinished(id: TestIdentifier, result: TestExecutionResult) {
        if (enabled) tracker.event("finished: ${id.displayName}", System.nanoTime())
    }

    companion object {
        private val enabled = System.getProperty("xtdb.test.hang-watchdog.enabled", "true").toBoolean()
        private val timeoutSeconds = System.getProperty("xtdb.test.hang-watchdog.timeout-seconds", "240").toLong()

        private val tracker = HangTracker(TimeUnit.SECONDS.toNanos(timeoutSeconds))

        // NOT System.out/err: Gradle's test workers replace those to capture per-test output, and
        // a hung test's captured output is never flushed to report or console — the dump would
        // vanish. Write to the process's real stdout FD — the route log4j's console appender
        // takes, and the only channel proven to stream into the CI log live.
        private val out = PrintStream(FileOutputStream(FileDescriptor.out), true)

        private val scannerStarted = AtomicBoolean(false)

        @OptIn(ExperimentalCoroutinesApi::class)
        private val probesInstalled get() = DebugProbes.isInstalled

        @OptIn(ExperimentalCoroutinesApi::class)
        private fun ensureScanner() {
            if (!scannerStarted.compareAndSet(false, true)) return

            // unconditional, so a silent watchdog is distinguishable from an unloaded one
            out.println("[hang-watchdog] armed (timeout ${timeoutSeconds}s, coroutine probes installed: $probesInstalled)")

            // off: creation traces capture a throwable per launch — ruinous for the sims, which
            // spawn coroutines in the hundreds of thousands. The dump's suspension stacks suffice.
            if (probesInstalled) DebugProbes.enableCreationStackTraces = false

            Executors.newSingleThreadScheduledExecutor { r ->
                Thread(r, "xtdb-test-hang-watchdog").apply { isDaemon = true }
            }.scheduleWithFixedDelay(::scan, 30, 30, TimeUnit.SECONDS)
        }

        private fun scan() {
            // scheduleWithFixedDelay permanently cancels the task if an execution throws — a freak
            // dump failure must not silence the watchdog for the rest of the JVM.
            try {
                tracker.overdue(System.nanoTime())?.let(::dump)
            } catch (t: Throwable) {
                try {
                    out.println("[hang-watchdog] scan failed: $t")
                } catch (_: Throwable) {
                }
            }
        }

        @OptIn(ExperimentalCoroutinesApi::class)
        private fun dump(o: HangTracker.Overdue) {
            out.println("=== [hang-watchdog] no test activity for ${TimeUnit.NANOSECONDS.toSeconds(o.quietNanos)}s (report ${o.report}; last event: ${o.lastEvent}) ===")

            if (probesInstalled) {
                try {
                    DebugProbes.dumpCoroutines(out)
                } catch (e: Throwable) {
                    out.println("[hang-watchdog] coroutine dump failed: $e")
                }
            }

            out.println("--- [hang-watchdog] thread dump ---")
            // ThreadInfo.toString truncates stacks, so format by hand.
            for (ti in ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
                out.println(buildString {
                    append("\"${ti.threadName}\" #${ti.threadId} ${ti.threadState}")
                    ti.lockName?.let { append(" on $it") }
                    ti.lockOwnerName?.let { append(" owned by \"$it\" #${ti.lockOwnerId}") }
                })
                ti.stackTrace.forEach { out.println("\tat $it") }
                out.println()
            }
            out.println("=== [hang-watchdog] end of dump (report ${o.report}) ===")
            out.flush()
        }
    }
}
