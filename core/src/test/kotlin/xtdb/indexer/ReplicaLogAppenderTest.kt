package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.log.ReplicaMessage.NoOp

class ReplicaLogAppenderTest {

    private class TriggeredElectionDriver : ElectionDriver {
        val trigger = Channel<Unit>()

        override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) = trigger.onReceive { body() }
    }

    @Test
    fun `an idle leader asserts, stamped with its own term`() = runTest {
        val logsDriver = RecordingLogsDriver()
        val election = TriggeredElectionDriver()
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 7, election)

        backgroundScope.launch { appender.run() }

        election.trigger.send(Unit)
        // A rendezvous send: this one only lands once the appender has looped back and re-armed, which it does after appending the first.
        election.trigger.send(Unit)

        assertEquals(
            NoOp(termId = 7), logsDriver.appended.first(),
            "a stale leader's assertions have to be fenced like its writes"
        )
    }

    @Test
    fun `a leader with traffic to append does not assert`() = runTest {
        val logsDriver = RecordingLogsDriver()
        val election = TriggeredElectionDriver()
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 1, election)

        appender.append(ControlItem(NoOp(srcMsgId = 42, termId = 1)))

        backgroundScope.launch { appender.run() }
        election.trigger.send(Unit)

        assertEquals(
            listOf(NoOp(srcMsgId = 42, termId = 1), NoOp(termId = 1)), logsDriver.appended,
            "the queued append is taken first, and the assertion only once nothing is queued"
        )
    }

    @Test
    fun `the shutdown cause unwinds the append loop`() = runTest {
        val appender = ReplicaLogAppender(RecordingLogsDriver(), leaderTerm = 1, NoAssertElectionDriver)
        val cause = RuntimeException("term failed")

        appender.shutdown(cause)

        val thrown = assertThrows<CancellationException> { appender.run() }

        // Walked rather than compared against `thrown.cause`: kotlinx recovers the stack trace by rethrowing a copy with the original as its cause, so the depth isn't ours to predict.
        assertTrue(
            generateSequence(thrown as Throwable) { it.cause }.any { it === cause },
            "a term that failed must not look like a clean exit"
        )
    }
}
