package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.NoOp
import java.time.Instant

class ReplicaLogAppenderTest {

    @Test
    fun `an idle leader asserts, stamped with its own term`() = runTest {
        val logsDriver = RecordingLogsDriver()
        val election = TriggeredElectionDriver()
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 7, election, pipelined = false)

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
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 1, election, pipelined = false)

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
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 4, TriggeredElectionDriver(), pipelined = false)

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

    private class HeldLogsDriver : LogProcessor.LogsDriver {
        val enqueued = mutableListOf<ReplicaMessage>()
        val handles = mutableListOf<CompletableDeferred<Log.MessageMetadata>>()

        override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> {
            enqueued += msg
            return CompletableDeferred<Log.MessageMetadata>().also { handles += it }
        }

        override suspend fun requestFlushBlock(expectedBlockIdx: Long) = error("unused")
    }

    @Test
    fun `an awaiting leader sends nothing further until its last record is durable`() = runTest {
        val logsDriver = HeldLogsDriver()
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 1, TriggeredElectionDriver(), pipelined = false)

        appender.append(ControlItem(NoOp(srcMsgId = 1, termId = 1)))
        appender.append(ControlItem(NoOp(srcMsgId = 2, termId = 1)))
        backgroundScope.launch { appender.run() }
        testScheduler.runCurrent()

        assertEquals(1, logsDriver.enqueued.size)

        logsDriver.handles.single().complete(Log.MessageMetadata(0, 0, Instant.EPOCH))
        testScheduler.runCurrent()

        assertEquals(2, logsDriver.enqueued.size)
    }

    @Test
    fun `a pipelined leader sends each record without waiting for the last to be durable`() = runTest {
        val logsDriver = HeldLogsDriver()
        val appender = ReplicaLogAppender(logsDriver, leaderTerm = 1, TriggeredElectionDriver(), pipelined = true)

        appender.append(ControlItem(NoOp(srcMsgId = 1, termId = 1)))
        appender.append(ControlItem(NoOp(srcMsgId = 2, termId = 1)))
        backgroundScope.launch { appender.run() }
        testScheduler.runCurrent()

        assertEquals(listOf(1L, 2L), logsDriver.enqueued.map { it.termSeq })
    }
}
