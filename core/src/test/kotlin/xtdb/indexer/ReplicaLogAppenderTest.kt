package xtdb.indexer

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.LeaderTerm
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.RelationReader
import xtdb.types.MessageId
import java.time.Instant

class ReplicaLogAppenderTest {

    private class RecordingDriver : LeaderDriver {
        val appended = mutableListOf<ReplicaMessage>()
        private var offset = -1L

        override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata {
            appended += msg
            return Log.MessageMetadata(0, ++offset, Instant.EPOCH)
        }

        override suspend fun applyTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) =
            error("not exercised")

        override suspend fun uploadBlock(
            boundaryMsgId: MessageId, termId: Long, boundary: ReplicaMessage.BlockBoundary,
        ): MessageId = error("not exercised")

        override suspend fun requestFlushBlock(expectedBlockIdx: Long): MessageId = error("not exercised")
    }

    private class TriggeredElectionDriver : ElectionDriver {
        val trigger = Channel<Unit>()

        override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) = trigger.onReceive { body() }

        override fun electionTimeout() = error("no reader here")
    }

    @Test
    fun `an idle leader asserts, stamped with its own term`() = runTest {
        val term = LeaderTerm.of(0, 7)
        val leaderDriver = RecordingDriver()
        val election = TriggeredElectionDriver()
        val appender = ReplicaLogAppender(leaderDriver, term, election)

        backgroundScope.launch { appender.run() }

        election.trigger.send(Unit)
        // A rendezvous send: this one only lands once the appender has looped back and re-armed,
        // which it does after appending the first.
        election.trigger.send(Unit)

        val asserted = leaderDriver.appended.first()
        assertInstanceOf(ReplicaMessage.NoOp::class.java, asserted)
        assertEquals(
            term, asserted.termId,
            "a stale leader's assertions have to be fenced like its writes"
        )
    }

    @Test
    fun `a leader with traffic to append does not assert`() = runTest {
        val leaderDriver = RecordingDriver()
        val election = TriggeredElectionDriver()
        val appender = ReplicaLogAppender(leaderDriver, LeaderTerm.of(0, 1), election)

        appender.append(ControlItem(ReplicaMessage.NoOp(srcMsgId = 42, termId = LeaderTerm.of(0, 1))))

        backgroundScope.launch { appender.run() }
        election.trigger.send(Unit)

        assertEquals(
            listOf<Long?>(42, null), leaderDriver.appended.map { (it as ReplicaMessage.NoOp).srcMsgId },
            "the queued append is taken first, and the assertion only once nothing is queued"
        )
    }

    @Test
    fun `the shutdown cause unwinds the append loop`() = runTest {
        val appender = ReplicaLogAppender(RecordingDriver(), LeaderTerm.of(0, 1), NoAssertElectionDriver)
        val cause = RuntimeException("term failed")

        appender.shutdown(cause)

        val thrown = assertThrows<CancellationException> { appender.run() }

        // Walked rather than compared against `thrown.cause`: kotlinx recovers the stack trace by
        // rethrowing a copy with the original as its cause, so the depth isn't ours to predict.
        assertTrue(
            generateSequence(thrown as Throwable) { it.cause }.any { it === cause },
            "a term that failed must not look like a clean exit"
        )
    }
}
