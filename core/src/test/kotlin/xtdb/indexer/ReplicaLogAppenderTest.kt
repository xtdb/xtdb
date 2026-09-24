package xtdb.indexer

import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.log.ReplicaMessage.NoOp

class ReplicaLogAppenderTest {

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
            NoOp(termId = 7, termSeq = 1), logsDriver.appended.first(),
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
            listOf(NoOp(srcMsgId = 42, termId = 1, termSeq = 1), NoOp(termId = 1, termSeq = 2)), logsDriver.appended,
            "the queued append is taken first, and the assertion only once nothing is queued"
        )
    }

    @Test
    fun `direct and queued appends share one run of positions from 1, direct ones needing no pump`() = runTest {
        val logsDriver = RecordingLogsDriver()
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 4, TriggeredElectionDriver())

        val direct = appender.appendNow(NoOp(srcMsgId = 1, termId = 4))

        appender.append(ControlItem(NoOp(srcMsgId = 2, termId = 4)))
        backgroundScope.launch { appender.run() }
        testScheduler.runCurrent()

        appender.appendNow(NoOp(srcMsgId = 3, termId = 4))
        appender.append(ControlItem(NoOp(srcMsgId = 4, termId = 4)))
        testScheduler.runCurrent()

        assertEquals(listOf(1L, 2L, 3L, 4L), logsDriver.appended.map { it.termSeq })
        assertEquals(listOf(1L, 2L, 3L, 4L), logsDriver.appended.map { (it as NoOp).srcMsgId })
        assertEquals(0L, direct.logOffset, "a direct append returns where the log put it")
    }
}
