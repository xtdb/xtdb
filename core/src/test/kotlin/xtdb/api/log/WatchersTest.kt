package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

class WatchersTest {

    @Test
    fun `test awaitTx ready already`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(3)
        val job = async { watchers.awaitTx(2) }
        while (!job.isCompleted) yield()
        assertNull(job.await())
    }

    @Test
    fun `test awaitTx waits`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(3)
        assertThrows<TimeoutCancellationException> { withTimeout(500) { watchers.awaitTx(4) } }
    }

    @Test
    fun `notifyTx resumes tx watchers`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(3)
        val await5 = async { watchers.awaitTx(5) }
        val await4 = async { watchers.awaitTx(4) }

        assertThrows<TimeoutCancellationException> { withTimeout(50) { await5.await() } }
        assertThrows<TimeoutCancellationException> { withTimeout(50) { await4.await() } }

        val res4 = TransactionCommitted(4, Instant.parse("2021-01-01T00:00:00Z"))
        watchers.notifyTx(res4, 4)

        assertThrows<TimeoutCancellationException> { withTimeout(50) { await5.await() } }
        assertEquals(res4, await4.await())

        val res5 = TransactionAborted(5, Instant.parse("2021-01-02T00:00:00Z"), Exception("test"))
        watchers.notifyTx(res5, 5)

        assertEquals(res5, await5.await())
    }

    @Test
    fun `notifyMsg advances specified watermarks`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // source-only: advances source, not tx
        val awaitSource1 = async { watchers.awaitSource(5) }
        watchers.notifyMsg(5)
        awaitSource1.await()
        assertThrows<TimeoutCancellationException> { withTimeout(50) { watchers.awaitTx(5) } }

        // both: advances source, not tx
        val awaitSource2 = async { watchers.awaitSource(8) }
        watchers.notifyMsg(8)
        awaitSource2.await()
        assertThrows<TimeoutCancellationException> { withTimeout(50) { watchers.awaitTx(8) } }
    }

    @Test
    fun `handles ingestion stopped`() = runTest(timeout = 1.seconds) {
        supervisorScope {
            val watchers = Watchers(3)
            val awaitTx = async { watchers.awaitTx(4) }
            val awaitSource = async { watchers.awaitSource(4) }

            assertThrows<TimeoutCancellationException> { withTimeout(50) { awaitTx.await() } }

            val ex = Exception("test")
            watchers.notifyError(ex)

            assertThrows<IngestionStoppedException> { awaitTx.await() }
                .also { assertEquals(ex, it.cause) }

            assertThrows<IngestionStoppedException> { awaitSource.await() }
                .also { assertEquals(ex, it.cause) }

            assertThrows<IngestionStoppedException> { watchers.awaitTx(5) }
                .also { assertEquals(ex, it.cause) }

            assertThrows<IngestionStoppedException> { watchers.awaitSource(5) }
                .also { assertEquals(ex, it.cause) }
        }
    }
}
