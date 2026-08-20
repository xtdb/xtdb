package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

class WatchersTest {

    @Test
    fun `test awaitTx ready already`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(latestTxId = 3, latestSourceMsgId = 3, latestReplicaMsgId = -1)
        val job = async { watchers.awaitTx(2) }
        while (!job.isCompleted) yield()
        assertNull(job.await())
    }

    @Test
    fun `test awaitTx waits`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(latestTxId = 3, latestSourceMsgId = 3, latestReplicaMsgId = -1)
        assertThrows<TimeoutCancellationException> { withTimeout(500) { watchers.awaitTx(4) } }
    }

    @Test
    fun `an applied tx resumes tx watchers`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(latestTxId = 3, latestSourceMsgId = 3, latestReplicaMsgId = -1)
        val await5 = async { watchers.awaitTx(5) }
        val await4 = async { watchers.awaitTx(4) }

        assertThrows<TimeoutCancellationException> { withTimeout(50) { await5.await() } }
        assertThrows<TimeoutCancellationException> { withTimeout(50) { await4.await() } }

        val res4 = TransactionResult.Committed(TransactionKey(4, Instant.parse("2021-01-01T00:00:00Z")))
        watchers.notifyApplied(null, 4, res4)

        assertThrows<TimeoutCancellationException> { withTimeout(50) { await5.await() } }
        assertEquals(res4, await4.await())

        val res5 = TransactionResult.Aborted(TransactionKey(5, Instant.parse("2021-01-02T00:00:00Z")), Exception("test"))
        watchers.notifyApplied(null, 5, res5)

        assertEquals(res5, await5.await())
    }

    @Test
    fun `seeded latestTxId can lag latestSourceMsgId`() = runTest(timeout = 1.seconds) {
        // Production shape from #5580: ext-source databases persist `latestProcessedMsgId` via
        // the block catalog (a source-msg-id watermark) and `latestCompletedTx` separately (a
        // tx-id watermark). On restart the source watermark is generally ahead of the tx
        // watermark. Watchers must accept an incoming ResolvedTx whose txId == seedTxId+1 even
        // though srcMsgId is well below it; pre-fix, Database.open seeded `latestTxId` from
        // `sourceMsgId` and the next ResolvedTx tripped the `txId > latestTxId` invariant.
        val watchers = Watchers(latestTxId = 5, latestSourceMsgId = 100, latestReplicaMsgId = -1)

        val tx6 = TransactionResult.Committed(TransactionKey(6, Instant.parse("2026-05-01T00:00:00Z")))
        // ext-source path: srcMsgId stays at the prior latestSourceMsgId.
        watchers.notifyApplied(null, watchers.latestSourceMsgId, tx6)

        assertEquals(tx6, watchers.awaitTx(6))
    }

    @Test
    fun `an advance without a tx moves only the source watermark`() = runTest(timeout = 1.seconds) {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val awaitSource1 = async { watchers.awaitSource(5) }
        watchers.notifyApplied(null, 5)
        awaitSource1.await()
        assertThrows<TimeoutCancellationException> { withTimeout(50) { watchers.awaitTx(5) } }

        val awaitSource2 = async { watchers.awaitSource(8) }
        watchers.notifyApplied(null, 8)
        awaitSource2.await()
        assertThrows<TimeoutCancellationException> { withTimeout(50) { watchers.awaitTx(8) } }
    }

    @Test
    fun `handles ingestion stopped`() = runTest(timeout = 1.seconds) {
        supervisorScope {
            val watchers = Watchers(latestTxId = 3, latestSourceMsgId = 3, latestReplicaMsgId = -1)
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
