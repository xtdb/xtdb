package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

class WatchersTest {

    @Test
    fun `test ready already`() = runTest(timeout = 1.seconds) {
        Watchers(3).use {
            val job = async { it.await0(2) }
            while (!job.isCompleted) yield()
            assertNull(job.await())
        }
    }

    @Test
    fun `test awaits`() = runTest(timeout = 1.seconds) {
        Watchers(3).use {
            assertThrows<TimeoutCancellationException> { withTimeout(500) { it.await0(4) } }
        }
    }

    @Test
    fun `notifies watchers of completion`() = runTest(timeout = 1.seconds) {
        Watchers(3).use {
            val await5 = async { it.await0(5) }
            val await4 = async { it.await0(4) }

            assertThrows<TimeoutCancellationException> { withTimeout(50) { await5.await() } }
            assertThrows<TimeoutCancellationException> { withTimeout(50) { await4.await() } }

            val res4 = TransactionCommitted(4, Instant.parse("2021-01-01T00:00:00Z"))
            it.notify(4, res4)

            assertThrows<TimeoutCancellationException> { withTimeout(50) { await5.await() } }
            assertEquals(res4, await4.await())

            val res5 = TransactionAborted(5, Instant.parse("2021-01-02T00:00:00Z"), Exception("test"))
            it.notify(5, res5)

            assertEquals(res5, await5.await())
        }
    }

    @Test
    fun `handles ingestion stopped`() = runTest(timeout = 1.seconds) {
        Watchers(3).use { watchers ->
            val await4 = async(SupervisorJob()) { watchers.await0(4) }

            assertThrows<TimeoutCancellationException> { withTimeout(50) { await4.await() } }

            val ex = Exception("test")
            watchers.notify(4, ex)

            assertThrows<IngestionStoppedException> { await4.await() }
                .also { assertEquals(ex, it.cause) }

            assertThrows<IngestionStoppedException> { watchers.await0(5) }
                .also { assertEquals(ex, it.cause) }

        }
    }
}