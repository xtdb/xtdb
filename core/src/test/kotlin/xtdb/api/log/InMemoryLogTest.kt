package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
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
    fun `commit does not deadlock on a single-threaded dispatcher`() {
        // commit() uses runBlocking internally. The old Channel pipeline needed a
        // Dispatchers.Default thread to process the message, so a single-threaded
        // dispatcher would deadlock: runBlocking blocked the only thread while the
        // pipeline needed it to complete the deferred.
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

        try {
            val log = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)

            runBlocking(dispatcher) {
                withTimeout(5.seconds) {
                    log.openAtomicProducer("test").withTx { tx ->
                        tx.appendMessage(ReplicaMessage.NoOp())
                    }
                }
            }

            assertEquals(0, log.latestSubmittedOffset)
        } finally {
            dispatcher.close()
        }
    }
}
