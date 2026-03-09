package xtdb.api.log

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.assertThrows
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.api.TransactionAborted
import xtdb.api.TransactionCommitted
import xtdb.api.TransactionResult
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds

@Tag("property")
class WatchersSimTest : SimulationTestBase() {

    fun interface TxResolver {
        fun resolve(txId: MessageId, systemTime: Instant): TransactionResult?
    }

    private class TxProcessor(private val watchers: Watchers, private val resolver: TxResolver) {
        private val utc = ZoneId.of("UTC")

        private fun systemTime(txId: MessageId) =
            LocalDate.of(2020, 1, (1 + txId).toInt()).atStartOfDay(utc).toInstant()

        suspend fun run() {
            for (i in 0L..<10L) {
                yield()
                val res = try {
                    resolver.resolve(i, systemTime(i))
                } catch (e: Exception) {
                    watchers.notify0(i, e)
                    break
                }

                watchers.notify0(i, res)
            }
        }
    }

    private inner class Awaiters(private val watchers: Watchers) {
        val res = mutableListOf<Pair<MessageId, Result<TransactionResult?>>>()

        suspend fun run() {
            for (i in 0L..<10L) {
                yield()
                val msgId = rand.nextLong(i - 2, i + 4).coerceIn(0, 9)
                res += Pair(msgId, runCatching { watchers.await0(msgId) })
            }
        }
    }

    data class TxException(val txId: MessageId) : Exception()

    @RepeatableSimulationTest
    fun testWatchersNotifications(@Suppress("unused") iteration: Int) = runTest(timeout = 1.seconds) {
        val watchers = Watchers(-1, dispatcher)

        val txProc = TxProcessor(watchers) { txId, systemTime ->
            if (rand.nextBoolean()) TransactionCommitted(txId, systemTime)
            else TransactionAborted(txId, systemTime, TxException(txId))
        }

        val awaiters = Awaiters(watchers)

        launch(dispatcher) {
            launch { txProc.run() }
            launch { awaiters.run() }
        }.join()

        val res = awaiters.res
        assertEquals(10, res.size)
        for ((msgId, txRes) in res) {
            val unwrapRes = txRes.getOrThrow() ?: continue

            val txId = unwrapRes.txId
            assertEquals(msgId, txId) { "Expected txId $msgId but got $txId" }

            if (unwrapRes is TransactionAborted) assertEquals(TxException(txId), unwrapRes.error)
        }
    }

    @RepeatableSimulationTest
    fun `test exception propagation`(@Suppress("unused") iteration: Int) = runTest(timeout = 1.seconds) {
        val errorTxId = rand.nextLong(0, 10)
        val watchers = Watchers(-1, dispatcher)

        val txProc = TxProcessor(watchers) { txId, systemTime ->
            if (txId == errorTxId) throw TxException(txId) else TransactionCommitted(txId, systemTime)
        }

        val awaiters = Awaiters(watchers)

        launch(dispatcher) {
            launch { txProc.run() }
            launch { awaiters.run() }
        }.join()

        val res = awaiters.res
        assertEquals(10, res.size)

        // prop:
        // - everything after the failed watch should fail
        // - everything that's successful should have a txId less than errorTxId

        var failed = false
        for ((msgId, txRes) in res) {
            if (txRes.isFailure) failed = true

            if (failed) {
                assertTrue(txRes.isFailure) {
                    "msgId=$msgId, errorTx = $errorTxId, but got success ${txRes.getOrNull()}"
                }

                val cause = assertThrows<IngestionStoppedException> { txRes.getOrThrow() }.cause
                assertEquals(TxException(errorTxId), cause)
            } else {
                assertTrue(txRes.isSuccess)
                assertTrue(msgId < errorTxId) { "Expected msgId $msgId to be less than errorTxId $errorTxId" }
            }
        }
    }
}