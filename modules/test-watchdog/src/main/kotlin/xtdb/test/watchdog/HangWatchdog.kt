package xtdb.test.watchdog

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.debug.DebugProbes
import org.junit.platform.engine.TestExecutionResult
import org.junit.platform.launcher.TestExecutionListener
import org.junit.platform.launcher.TestIdentifier
import org.junit.platform.launcher.TestPlan
import java.io.PrintStream
import java.lang.management.ManagementFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Dumps coroutines and threads to stderr when a test runs past the hang threshold, so a hung
 * CI job names its stuck coroutine in the log instead of dying silently at the action timeout —
 * see xtdb#5711.
 *
 * Diagnostic only: it never fails or interrupts the test, so a slow-but-progressing test that
 * trips it costs nothing but log noise. Registered via ServiceLoader
 * (`META-INF/services/org.junit.platform.launcher.TestExecutionListener`), and this module rides
 * every project's testRuntimeOnly classpath, so every `Test` task gets it with no per-project
 * config.
 *
 * System properties: `xtdb.test.hang-watchdog.enabled` (default `true`),
 * `xtdb.test.hang-watchdog.timeout-seconds` (default 360 — comfortably past any legitimate
 * single test, comfortably before CI's 15-minute action timeout).
 */
class HangWatchdog : TestExecutionListener {

    private val enabled = System.getProperty("xtdb.test.hang-watchdog.enabled", "true").toBoolean()
    private val timeoutSeconds = System.getProperty("xtdb.test.hang-watchdog.timeout-seconds", "360").toLong()

    private val tracker = HangTracker(TimeUnit.SECONDS.toNanos(timeoutSeconds))

    // Gradle can execute several TestPlans in one worker JVM; install/schedule only once and let
    // the daemon scheduler live for the JVM's lifetime rather than tracking plan finish.
    private val active = AtomicBoolean(false)

    @Volatile
    private var probesInstalled = false

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun testPlanExecutionStarted(testPlan: TestPlan) {
        if (!enabled || !active.compareAndSet(false, true)) return

        try {
            // creation stack traces capture a throwable per coroutine launch — far too expensive
            // for the sim tests, and the dump's suspension stacks are what we need.
            DebugProbes.enableCreationStackTraces = false
            DebugProbes.install()
            probesInstalled = true
        } catch (e: Throwable) {
            // e.g. a JRE without jdk.attach — degrade to thread dumps rather than failing the run
            System.err.println("[hang-watchdog] DebugProbes unavailable ($e); will dump threads only")
        }

        Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "xtdb-test-hang-watchdog").apply { isDaemon = true }
        }.scheduleWithFixedDelay(::scan, 30, 30, TimeUnit.SECONDS)
    }

    override fun executionStarted(id: TestIdentifier) {
        if (enabled && id.isTest) tracker.started(id.uniqueId, id.displayName, System.nanoTime())
    }

    override fun executionFinished(id: TestIdentifier, result: TestExecutionResult) {
        if (enabled && id.isTest) tracker.finished(id.uniqueId)
    }

    private fun scan() {
        tracker.overdue(System.nanoTime()).forEach(::dump)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun dump(o: HangTracker.Overdue) {
        val out: PrintStream = System.err
        out.println("=== [hang-watchdog] '${o.displayName}' still running after ${TimeUnit.NANOSECONDS.toSeconds(o.elapsedNanos)}s (report ${o.report}) ===")

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
        out.println("=== [hang-watchdog] end of dump for '${o.displayName}' ===")
        out.flush()
    }
}
