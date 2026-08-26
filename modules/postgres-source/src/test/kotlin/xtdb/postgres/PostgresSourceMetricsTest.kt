package xtdb.postgres

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.tx.BlockDetails
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.postgres.proto.postgresSourceToken
import java.time.Instant

class PostgresSourceMetricsTest {

    /** Yields [txs] then parks, completing [parked] — so a test can observe a source mid-stream. */
    private class StubStream(
        private val txs: ArrayDeque<PostgresDriver.Transaction> = ArrayDeque(),
    ) : PostgresDriver.ChangeStream {
        val parked = CompletableDeferred<Unit>()

        override val walEnd get() = 0L
        override suspend fun acknowledge(lsn: Long) = Unit

        override suspend fun poll(): PostgresDriver.Transaction? {
            txs.removeFirstOrNull()?.let { return it }
            parked.complete(Unit)
            awaitCancellation()
        }

        override fun close() = Unit
    }

    /** Hands out [streams] in order, one per assignment. */
    private class StubDriver(
        private val streams: ArrayDeque<StubStream>,
        private val lagBytes: () -> Long? = { 8192L },
    ) : PostgresDriver {
        override fun openSnapshot(): PostgresDriver.SnapshotReader = error("resumes, so never snapshots")
        override suspend fun openStream(startLsn: Long) = streams.removeFirst()
        override fun publicationExists() = true
        override fun queryWalLagBytes() = lagBytes()
        override fun close() = Unit
    }

    private object StubIndexer : TxIndexer {
        override val latestBlock = MutableStateFlow<BlockDetails?>(null)

        private val txKey = object : TransactionKey {
            override val txId = 1L
            override val systemTime: Instant = Instant.EPOCH
        }

        override suspend fun executeTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
            writer: suspend (OpenTx) -> TxIndexer.TxResult,
        ): TransactionResult = TransactionResult.Committed(txKey)

        // completed on return, so the poll loop's drain runs on the next tick
        override suspend fun submitTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
            writer: suspend (OpenTx) -> TxIndexer.TxResult,
        ): Deferred<TransactionResult> = CompletableDeferred(TransactionResult.Committed(txKey))
    }

    // snapshotCompleted, so the assignment resumes straight into streaming
    private val resumeToken = postgresSourceToken {
        latestCommittedLsn = 0
        snapshotCompleted = true
    }.toByteArray()

    private fun openSource(reg: SimpleMeterRegistry, driver: PostgresDriver) =
        PostgresSource("xtdb", driver, "test_slot", DirectMirror(), reg)

    private fun SimpleMeterRegistry.walLag() =
        get("xtdb.postgres_source.wal_lag_bytes").gauge().value()

    private fun SimpleMeterRegistry.connectionState() =
        get("xtdb.postgres_source.connection_state").gauge().value()

    private fun SimpleMeterRegistry.lastEventTime() =
        get("xtdb.postgres_source.last_event_time").gauge().value()

    @Test
    fun `NaN until the slot is readable, and again once it isn't`() = runTest {
        val reg = SimpleMeterRegistry()
        var lag: Long? = null
        val stream = StubStream()

        openSource(reg, StubDriver(ArrayDeque(listOf(stream))) { lag }).use { source ->
            val assignment = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }
            stream.parked.await()

            assertTrue(reg.walLag().isNaN(), "no reading yet")

            lag = 0
            assertEquals(0.0, reg.walLag(), "0 is caught-up, once we've read it")

            lag = 8192
            assertEquals(8192.0, reg.walLag())

            lag = null
            assertTrue(reg.walLag().isNaN(), "slot gone — unknown, not caught up")

            assignment.cancelAndJoin()
        }
    }

    @Test
    fun `NaN when the query throws`() = runTest {
        val reg = SimpleMeterRegistry()
        var refuseConnection = false
        val stream = StubStream()

        val driver = StubDriver(ArrayDeque(listOf(stream))) {
            if (refuseConnection) throw IllegalStateException("connection refused") else 8192L
        }

        openSource(reg, driver).use { source ->
            val assignment = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }
            stream.parked.await()

            assertEquals(8192.0, reg.walLag())

            refuseConnection = true
            assertTrue(reg.walLag().isNaN(), "failed read — unknown, not caught up")

            assignment.cancelAndJoin()
        }
    }

    @Test
    fun `NaN while another node holds the partition`() = runTest {
        val reg = SimpleMeterRegistry()
        val stream = StubStream()
        var queried = false

        val driver = StubDriver(ArrayDeque(listOf(stream))) { queried = true; 8192L }

        openSource(reg, driver).use { source ->
            assertTrue(reg.walLag().isNaN(), "unassigned — this node doesn't hold the slot")
            assertEquals(0.0, reg.connectionState())
            assertFalse(queried, "an unassigned source must not connect to the upstream")

            val assignment = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }
            stream.parked.await()

            assertEquals(8192.0, reg.walLag(), "readable once assigned")
            assertEquals(1.0, reg.connectionState())

            assignment.cancelAndJoin()

            assertTrue(reg.walLag().isNaN(), "demoted — back to another node's slot")
            assertEquals(0.0, reg.connectionState())
        }
    }

    @Test
    fun `a new assignment doesn't inherit the last one's event time`() = runTest {
        val reg = SimpleMeterRegistry()
        val commitTime = Instant.parse("2026-08-26T09:00:00Z")

        val firstStream = StubStream(
            ArrayDeque(listOf(PostgresDriver.Transaction(lsn = 10, commitTime = commitTime, ops = emptyList())))
        )
        val secondStream = StubStream()

        openSource(reg, StubDriver(ArrayDeque(listOf(firstStream, secondStream)))).use { source ->
            assertEquals(0.0, reg.lastEventTime(), "no events before the first assignment")

            val first = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }
            firstStream.parked.await()
            assertEquals(commitTime.epochSecond.toDouble(), reg.lastEventTime(), "the applied commit's time")
            first.cancelAndJoin()

            val second = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }
            secondStream.parked.await()
            assertEquals(0.0, reg.lastEventTime(), "this assignment has applied nothing of its own")

            second.cancelAndJoin()
        }
    }
}
