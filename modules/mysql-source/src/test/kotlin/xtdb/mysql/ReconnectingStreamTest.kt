package xtdb.mysql

import xtdb.util.ReconnectPolicy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import xtdb.api.error.Unavailable
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class ReconnectingStreamTest {

    private val policy = ReconnectPolicy(initialDelay = 100.milliseconds, maxDelay = 1.seconds, resetAfter = 1.minutes, jitter = 0.0)

    private val source = UUID.randomUUID()

    private fun connectionLost() = Unavailable("binlog connection closed", "xtdb.mysql/binlog-connection-lost")

    private fun position(upTo: Long) = (1..upTo).fold(GtidSet.EMPTY) { set, seq -> set + Gtid(source, seq) }

    private fun tx(seq: Long) = MySqlDriver.Transaction.Committed(Gtid(source, seq), Instant.EPOCH, emptyList(), position(seq))

    private class ScriptedStream(steps: List<Any?>) : MySqlDriver.ChangeStream {
        private val steps = ArrayDeque(steps)
        var closed = false

        override val connected get() = !closed

        override suspend fun poll(): MySqlDriver.Transaction.Committed? = when (val s = if (steps.isEmpty()) null else steps.removeFirst()) {
            is Throwable -> throw s
            else -> s as MySqlDriver.Transaction.Committed?
        }

        override fun close() {
            closed = true
        }
    }

    private class ScriptedOpener(private val scope: TestScope, outcomes: List<Any>) {
        private val outcomes = ArrayDeque(outcomes)
        val froms = mutableListOf<GtidSet>()
        val openedAt = mutableListOf<Long>()

        suspend fun open(from: GtidSet): MySqlDriver.ChangeStream {
            froms += from
            openedAt += scope.currentTime
            return when (val o = outcomes.removeFirst()) {
                is Throwable -> throw o
                else -> o as MySqlDriver.ChangeStream
            }
        }
    }

    private suspend fun TestScope.open(opener: ScriptedOpener, from: GtidSet = GtidSet.EMPTY) =
        ReconnectingStream.open("xtdb", opener::open, from, policy, testScheduler.timeSource)

    private val ScriptedOpener.gaps get() = openedAt.zipWithNext { a, b -> (b - a).milliseconds }

    @Test
    fun `a stream that fails a hundred times in a row still reopens`() = runTest {
        val opener = ScriptedOpener(this, List(100) { connectionLost() } + ScriptedStream(listOf(tx(1))))

        assertEquals(tx(1), open(opener).poll())
        assertEquals(101, opener.froms.size)
    }

    @Test
    fun `the wait between reopens never exceeds the cap`() = runTest {
        val opener = ScriptedOpener(this, List(50) { connectionLost() } + ScriptedStream(emptyList()))

        open(opener)

        assertEquals(listOf(100, 200, 400, 800).map { it.milliseconds } + List(46) { policy.maxDelay }, opener.gaps)
    }

    @Test
    fun `a reopen starts after the last transaction presented`() = runTest {
        val first = ScriptedStream(listOf(tx(1), tx(2), connectionLost()))
        val opener = ScriptedOpener(this, listOf(first, ScriptedStream(listOf(tx(3)))))

        val stream = open(opener)

        assertEquals(listOf(tx(1), tx(2), tx(3)), List(3) { stream.poll() })
        assertEquals(listOf(GtidSet.EMPTY, position(2)), opener.froms)
        assertTrue(first.closed, "the broken stream is released")
    }

    @Test
    fun `a delivered transaction resets the wait`() = runTest {
        val opener = ScriptedOpener(
            this,
            List(10) { connectionLost() } +
                ScriptedStream(listOf(tx(1), connectionLost())) +
                ScriptedStream(emptyList()),
        )

        val stream = open(opener)
        assertEquals(tx(1), stream.poll())
        stream.poll()

        assertEquals(policy.initialDelay, opener.gaps.last())
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
        val opener = ScriptedOpener(this, listOf(ScriptedStream(listOf(Incorrect("purged", "xtdb.mysql/binlog-purged")))))

        val stream = open(opener)

        assertFailsWith<Incorrect> { stream.poll() }
        assertEquals(1, opener.froms.size)
    }

    @Test
    fun `a cancelled caller does not reopen`() = runTest {
        val opener = ScriptedOpener(this, List(10) { connectionLost() })

        val job = launch { open(opener) }
        delay(1)
        job.cancel(CancellationException("stood down"))
        advanceUntilIdle()

        assertEquals(1, opener.froms.size)
    }
}
