package xtdb.api.log

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.api.log.Log.Record
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

class LocalLogTest {

    @TempDir
    lateinit var tempDir: Path

    @Tag("integration")
    @RepeatedTest(5000)
    fun `close should cancel all subscription coroutines without leaking - N=1`() = runTest(timeout = 10.seconds) {
        closeCancelsSubscriptionCoroutines(partitions = 1)
    }

    @Tag("integration")
    @RepeatedTest(500)
    fun `close should cancel all subscription coroutines without leaking - N=2`() = runTest(timeout = 10.seconds) {
        closeCancelsSubscriptionCoroutines(partitions = 2)
    }

    private suspend fun TestScope.closeCancelsSubscriptionCoroutines(partitions: Int) {
        val log = LocalLog.Factory(tempDir.resolve("log-$partitions"))
            .openSourceLog(emptyMap(), partitions)

        val jobs = List(partitions) { p ->
            launch {
                log.tailAll(p, -1L, object : Log.RecordProcessor<SourceMessage> {
                    override suspend fun processRecords(records: List<Record<SourceMessage>>) = Unit
                })
            }
        }

        log.appendMessage(SourceMessage.FlushBlock(1), partition = 0)

        jobs.forEach { it.cancelAndJoin() }

        log.close()
    }

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage returns null when log is empty`() {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())
        log.use {
            assertNull(log.readLastMessage())
        }
    }

    @Test
    fun `readLastMessage returns the message after appending one`() = runTest {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())
        log.use {
            log.appendMessage(txMessage(1))

            val lastMessage = log.readLastMessage()
            assertNotNull(lastMessage)
            assertTrue(lastMessage is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 1), (lastMessage as SourceMessage.LegacyTx).payload)
        }
    }

    @Test
    fun `readLastMessage returns the last message after appending multiple`() = runTest {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())
        log.use {
            log.appendMessage(txMessage(1))
            log.appendMessage(txMessage(2))
            log.appendMessage(txMessage(3))

            val lastMessage = log.readLastMessage()
            assertNotNull(lastMessage)
            assertTrue(lastMessage is SourceMessage.LegacyTx)
            assertArrayEquals(byteArrayOf(-1, 3), (lastMessage as SourceMessage.LegacyTx).payload)
        }
    }

    @Test
    fun `writes to different partitions are isolated`() = runTest {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap(), partitions = 2)
        log.use {
            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 1)
            log.appendMessage(txMessage(3), partition = 0)

            val last0 = log.readLastMessage(0) as SourceMessage.LegacyTx
            val last1 = log.readLastMessage(1) as SourceMessage.LegacyTx
            assertArrayEquals(byteArrayOf(-1, 3), last0.payload)
            assertArrayEquals(byteArrayOf(-1, 2), last1.payload)
        }
    }

    @Test
    fun `offsets advance independently per partition`() = runTest {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap(), partitions = 2)
        log.use {
            val emptyOffset = log.latestSubmittedOffset(0)
            assertEquals(emptyOffset, log.latestSubmittedOffset(1))

            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 0)
            log.appendMessage(txMessage(3), partition = 1)

            // Partition 0 has advanced past partition 1 (two writes vs one); LocalLog offsets
            // are file positions, so ordering suffices as the isolation check.
            assertTrue(log.latestSubmittedOffset(0) > log.latestSubmittedOffset(1))
            assertTrue(log.latestSubmittedOffset(1) > emptyOffset)
        }
    }

    @Test
    fun `tailAll only sees its own partition`() = runTest(timeout = 5.seconds) {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap(), partitions = 2)
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

            assertTrue(ch0.tryReceive().isFailure, "partition 0's subscriber received a partition 1 write")
            assertTrue(ch1.tryReceive().isFailure, "partition 1's subscriber received a partition 0 write")

            job0.cancelAndJoin()
            job1.cancelAndJoin()
        }
    }

    @Test
    fun `openGroupSubscription drives the listener lifecycle for every partition`() = runTest(timeout = 5.seconds) {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap(), partitions = 3)
        log.use {
            val listener = RecordingListener<SourceMessage>()

            val job = backgroundScope.launch { log.openGroupSubscription(listener) }

            while (listener.committed.size < 3) yield()

            job.cancelAndJoin()

            assertEquals(setOf(0, 1, 2), listener.transitioned.toSet())
            assertEquals(setOf(0, 1, 2), listener.committed.toSet())
            assertEquals(setOf(0, 1, 2), listener.demoted.toSet())
        }
    }

    @Test
    fun `multi-partition files land under the nested layout`() = runTest {
        val root = tempDir.resolve("log")
        val log = LocalLog.Factory(root).openSourceLog(emptyMap(), partitions = 2)
        log.use {
            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 1)

            // N>1 nests under a role directory.
            assertTrue(Files.isDirectory(root.resolve("LOG")), "root/LOG should be a directory at N>1")
            assertTrue(Files.isRegularFile(root.resolve("LOG/0")), "root/LOG/0 should exist")
            assertTrue(Files.isRegularFile(root.resolve("LOG/1")), "root/LOG/1 should exist")
        }
    }

    @Test
    fun `single-partition uses the pre-#5557 bare LOG file`() = runTest {
        val root = tempDir.resolve("log")
        val log = LocalLog.Factory(root).openSourceLog(emptyMap())  // partitions = 1 (default)
        log.use {
            log.appendMessage(txMessage(1))

            // N=1 keeps the pre-#5557 layout — a plain file at rootPath.
            assertTrue(Files.isRegularFile(root.resolve("LOG")), "root/LOG should be a file at N=1")
        }
    }

    @Test
    fun `restart recovers per-partition state from disk`() = runTest {
        val root = tempDir.resolve("log")
        val factory = LocalLog.Factory(root)

        factory.openSourceLog(emptyMap(), partitions = 2).use { log ->
            log.appendMessage(txMessage(1), partition = 0)
            log.appendMessage(txMessage(2), partition = 1)
            log.appendMessage(txMessage(3), partition = 0)
        }

        factory.openSourceLog(emptyMap(), partitions = 2).use { log ->
            val last0 = log.readLastMessage(0) as SourceMessage.LegacyTx
            val last1 = log.readLastMessage(1) as SourceMessage.LegacyTx
            assertArrayEquals(byteArrayOf(-1, 3), last0.payload)
            assertArrayEquals(byteArrayOf(-1, 2), last1.payload)

            assertTrue(log.latestSubmittedOffset(0) > log.latestSubmittedOffset(1))
        }
    }

    private class RecordingListener<M> : Log.SubscriptionListener<M> {
        val transitioned = mutableListOf<Int>()
        val committed = mutableListOf<Int>()
        val demoted = mutableListOf<Int>()

        override fun launchTransition(partition: Int): Deferred<Unit> {
            transitioned.add(partition)
            return CompletableDeferred(Unit)
        }

        override fun commitLeader(partition: Int): Log.TailSpec<M> {
            committed.add(partition)
            return Log.TailSpec(-1L) { _ -> }
        }

        override suspend fun demoteLeader(partition: Int) {
            demoted.add(partition)
        }
    }
}
