package xtdb.indexer

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
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
}
