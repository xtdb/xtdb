package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.Log.Record
import java.time.InstantSource
import java.util.concurrent.Executors
import kotlin.time.Duration.Companion.seconds

class InMemoryLogTest {

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage always returns null`() = runTest {
        val log = InMemoryLog.Factory().openSourceLog(emptyMap())
        log.use {
            assertNull(log.readLastMessage())

            log.appendMessage(txMessage(1))

            // Still null because InMemoryLog has no persistence
            assertNull(log.readLastMessage())
        }
    }

    @Test
    fun `appendMessage does not deadlock on a single-threaded dispatcher`() {
        // appendMessage must complete on the calling thread alone. An earlier Channel pipeline needed a
        // Dispatchers.Default thread to process the message, so a single-threaded caller deadlocked.
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

        try {
            val log = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)

            runBlocking(dispatcher) {
                withTimeout(5.seconds) {
                    log.appendMessage(ReplicaMessage.NoOp())
                }
            }

            assertEquals(0, log.latestSubmittedOffset())
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `writes to different partitions are isolated`() = runTest {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 2)
        log.use {
            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 1)

            // InMemoryLog has no readLastMessage backing — use readRecords to prove per-partition
            // isolation. Bound the scan by each partition's latest msgId so the epoch guard passes.
            val p0 = log.readRecords(0, 0L, log.latestSubmittedMsgId(0) + 1).toList()
            val p1 = log.readRecords(1, 0L, log.latestSubmittedMsgId(1) + 1).toList()

            assertEquals(1, p0.size)
            assertEquals(1, p1.size)
            assertArrayEquals(byteArrayOf(-1, 1), (p0[0].message as SourceMessage.LegacyTx).payload)
            assertArrayEquals(byteArrayOf(-1, 2), (p1[0].message as SourceMessage.LegacyTx).payload)

            assertEquals(0L, log.latestSubmittedOffset(0))
            assertEquals(0L, log.latestSubmittedOffset(1))
        }
    }

    @Test
    fun `offsets advance independently per partition`() = runTest {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 2)
        log.use {
            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 0)
            log.appendMessage(txMessage(3), partition = 0)
            log.appendMessage(txMessage(4), partition = 1)
            log.appendMessage(txMessage(5), partition = 1)

            assertEquals(2L, log.latestSubmittedOffset(0))
            assertEquals(1L, log.latestSubmittedOffset(1))
        }
    }

    @Test
    fun `tailAll only sees its own partition`() = runTest(timeout = 5.seconds) {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 2)
        log.use {
            val ch0 = Channel<Record<SourceMessage>>(Channel.UNLIMITED)
            val ch1 = Channel<Record<SourceMessage>>(Channel.UNLIMITED)

            val job0 = backgroundScope.launch {
                log.tailAll(0, -1L) { recs -> recs.forEach { ch0.send(it) } }
            }
            val job1 = backgroundScope.launch {
                log.tailAll(1, -1L) { recs -> recs.forEach { ch1.send(it) } }
            }

            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 1)

            val r0 = ch0.receive()
            val r1 = ch1.receive()

            assertArrayEquals(byteArrayOf(-1, 1), (r0.message as SourceMessage.LegacyTx).payload)
            assertArrayEquals(byteArrayOf(-1, 2), (r1.message as SourceMessage.LegacyTx).payload)

            // No cross-delivery: nothing else waiting on either channel.
            assertTrue(ch0.tryReceive().isFailure, "partition 0's subscriber received a partition 1 write")
            assertTrue(ch1.tryReceive().isFailure, "partition 1's subscriber received a partition 0 write")

            job0.cancelAndJoin()
            job1.cancelAndJoin()
        }
    }

    @Test
    fun `a quiet log reports that a read found nothing`() = runTest(timeout = 5.seconds) {
        InMemoryLog<SourceMessage>(InstantSource.system(), 0).use { log ->
            val emptyReads = Channel<Unit>(Channel.UNLIMITED)

            val job = backgroundScope.launch {
                log.tailAll(0, -1) { records -> if (records.isEmpty()) emptyReads.send(Unit) }
            }

            // Election is timed across a participant's own reads, so a log with nothing to say still
            // has to say so — otherwise a node no longer being delivered to looks identical to an idle one.
            withContext(Dispatchers.Default) { emptyReads.receive() }

            job.cancelAndJoin()
        }
    }
}
