package xtdb.postgres

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.postgresql.util.PSQLException
import org.postgresql.util.PSQLState
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class ReconnectingStreamTest {

    private val policy = ReconnectPolicy(initialDelay = 100.milliseconds, maxDelay = 1.seconds, resetAfter = 1.minutes, jitter = 0.0)

    private fun connectionLost() = PSQLException("Database connection failed when reading from copy", PSQLState.CONNECTION_FAILURE)

    private fun tx(lsn: Long) = PostgresDriver.Transaction(lsn, Instant.EPOCH, emptyList())

    /** Plays out [steps] in order, one per call to [poll] or [acknowledge]: a transaction, an idle tick, or a throw. */
    private class ScriptedStream(steps: List<Any?>, override val walEnd: Long = 0) : PostgresDriver.ChangeStream {
        private val steps = ArrayDeque(steps)
        var closed = false

        override val connected get() = !closed

        private fun next(): Any? = if (steps.isEmpty()) null else steps.removeFirst()

        override suspend fun poll(): PostgresDriver.Transaction? = when (val s = next()) {
            is Throwable -> throw s
            else -> s as PostgresDriver.Transaction?
        }

        override suspend fun acknowledge(lsn: Long) {
            (next() as? Throwable)?.let { throw it }
        }

        override fun close() {
            closed = true
        }
    }

    /** Hands out [outcomes] in order — a stream, or a throw — recording the LSN and virtual time of every open. */
    private class ScriptedOpener(private val scope: TestScope, outcomes: List<Any>) {
        private val outcomes = ArrayDeque(outcomes)
        val startLsns = mutableListOf<Long>()
        val openedAt = mutableListOf<Long>()

        suspend fun open(startLsn: Long): PostgresDriver.ChangeStream {
            startLsns += startLsn
            openedAt += scope.currentTime
            return when (val o = outcomes.removeFirst()) {
                is Throwable -> throw o
                else -> o as PostgresDriver.ChangeStream
            }
        }
    }

    private suspend fun TestScope.open(opener: ScriptedOpener, startLsn: Long = 0) =
        ReconnectingStream.open("xtdb", opener::open, startLsn, policy, testScheduler.timeSource)

    private val ScriptedOpener.gaps get() = openedAt.zipWithNext { a, b -> (b - a).milliseconds }

    @Test
    fun `a stream that fails a hundred times in a row still reopens`() = runTest {
        val opener = ScriptedOpener(this, List(100) { connectionLost() } + ScriptedStream(listOf(tx(10))))

        val stream = open(opener)

        assertEquals(tx(10), stream.poll())
        assertEquals(101, opener.startLsns.size)
    }

    @Test
    fun `the wait between reopens never exceeds the cap`() = runTest {
        val opener = ScriptedOpener(this, List(50) { connectionLost() } + ScriptedStream(emptyList()))

        open(opener)

        assertEquals(listOf(100, 200, 400, 800).map { it.milliseconds } + List(46) { policy.maxDelay }, opener.gaps)
    }

    @Test
    fun `a reopen resumes after the last transaction presented, and skips what it re-sends`() = runTest {
        val opener = ScriptedOpener(
            this, listOf(
                ScriptedStream(listOf(tx(10), tx(20), connectionLost())),
                ScriptedStream(listOf(tx(15), tx(20), tx(30))),
            )
        )

        val stream = open(opener, startLsn = 5)

        assertEquals(listOf(tx(10), tx(20), tx(30)), List(3) { stream.poll() })
        assertEquals(listOf(5L, 20L), opener.startLsns)
    }

    @Test
    fun `a failed acknowledge reopens the stream`() = runTest {
        val first = ScriptedStream(listOf(connectionLost()))
        val opener = ScriptedOpener(this, listOf(first, ScriptedStream(emptyList())))

        open(opener).acknowledge(10)

        assertTrue(first.closed, "the broken stream is released")
        assertEquals(2, opener.startLsns.size)
    }

    @Test
    fun `a delivered transaction resets the wait`() = runTest {
        val opener = ScriptedOpener(
            this,
            List(10) { connectionLost() } +
                ScriptedStream(listOf(tx(10), connectionLost())) +
                ScriptedStream(emptyList()),
        )

        val stream = open(opener)
        assertEquals(tx(10), stream.poll())
        stream.poll()

        assertEquals(policy.initialDelay, opener.gaps.last())
    }

    @Test
    fun `a quiet spell after a reopen resets the wait`() = runTest {
        val opener = ScriptedOpener(
            this,
            List(10) { connectionLost() } +
                ScriptedStream(listOf(null, connectionLost())) +
                ScriptedStream(emptyList()),
        )

        val stream = open(opener)
        assertEquals(null, stream.poll())
        delay(policy.resetAfter)
        val failedAt = currentTime
        stream.poll()

        assertEquals(policy.initialDelay, (opener.openedAt.last() - failedAt).milliseconds)
    }

    @Test
    fun `a stream that fails again before the quiet spell keeps backing off`() = runTest {
        val opener = ScriptedOpener(
            this,
            List(10) { connectionLost() } +
                ScriptedStream(listOf(null, connectionLost())) +
                ScriptedStream(emptyList()),
        )

        val stream = open(opener)
        assertEquals(null, stream.poll())
        val failedAt = currentTime
        stream.poll()

        assertEquals(policy.maxDelay, (opener.openedAt.last() - failedAt).milliseconds)
    }

    @Test
    fun `an outage that starts after a quiet spell still backs off`() = runTest {
        val opener = ScriptedOpener(
            this,
            listOf(connectionLost()) +
                ScriptedStream(listOf(null, connectionLost())) +
                List(10) { connectionLost() } +
                ScriptedStream(emptyList()),
        )

        val stream = open(opener)
        assertEquals(null, stream.poll())
        delay(policy.resetAfter)
        stream.poll()

        assertEquals(policy.maxDelay, opener.gaps.last())
    }

    @Test
    fun `the stream reports itself disconnected while a reopen is pending`() = runTest {
        val opener = ScriptedOpener(this, listOf(ScriptedStream(listOf(connectionLost())), ScriptedStream(emptyList())))

        val stream = open(opener)
        assertTrue(stream.connected)

        launch { stream.poll() }
        runCurrent()
        assertFalse(stream.connected, "between the failure and the reopen")

        advanceUntilIdle()
        assertTrue(stream.connected, "once reopened")
    }

    @Test
    fun `a failure that isn't the connection's is not retried`() = runTest {
        val opener = ScriptedOpener(this, listOf(ScriptedStream(listOf(IllegalStateException("decode failed")))))

        val stream = open(opener)

        assertFailsWith<IllegalStateException> { stream.poll() }
        assertEquals(1, opener.startLsns.size)
    }

    @Test
    fun `a cancelled caller does not reopen`() = runTest {
        val opener = ScriptedOpener(this, List(10) { connectionLost() })

        val job = launch { open(opener) }
        delay(1)
        job.cancel(CancellationException("stood down"))
        advanceUntilIdle()

        assertEquals(1, opener.startLsns.size)
    }
}
