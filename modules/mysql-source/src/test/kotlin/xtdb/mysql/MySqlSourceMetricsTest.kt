package xtdb.mysql

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
import xtdb.mysql.proto.mySqlSourceToken
import java.time.Instant
import java.util.UUID

class MySqlSourceMetricsTest {

    private val upstream = UUID.randomUUID()

    private fun position(upTo: Long) = (1..upTo).fold(GtidSet.EMPTY) { set, seq -> set + Gtid(upstream, seq) }

    private class StubStream(
        private val txs: ArrayDeque<MySqlDriver.Transaction.Committed> = ArrayDeque(),
    ) : MySqlDriver.ChangeStream {
        val parked = CompletableDeferred<Unit>()

        @Volatile override var connected = true

        override suspend fun poll(): MySqlDriver.Transaction.Committed? {
            txs.removeFirstOrNull()?.let { return it }
            parked.complete(Unit)
            awaitCancellation()
        }

        override fun close() = Unit
    }

    private class StubDriver(
        private val streams: ArrayDeque<StubStream>,
        private val executed: () -> GtidSet,
    ) : MySqlDriver {
        override fun checkUpstream() = Unit
        override fun openSnapshot(): MySqlDriver.SnapshotReader = error("resumes, so never snapshots")
        override suspend fun openStream(from: GtidSet) = streams.removeFirst()
        override fun queryExecutedGtids() = executed()
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

        override suspend fun submitTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
            writer: suspend (OpenTx) -> TxIndexer.TxResult,
        ): Deferred<TransactionResult> = CompletableDeferred(TransactionResult.Committed(txKey))
    }

    private fun resumeToken(from: GtidSet) =
        mySqlSourceToken { gtidSet = from.toString(); snapshotCompleted = true }.toByteArray()

    private fun openSource(reg: SimpleMeterRegistry, driver: MySqlDriver) = MySqlSource("xtdb", "mysql", driver, reg)

    private fun SimpleMeterRegistry.gtidLag() = get("xtdb.mysql_source.gtid_lag").gauge().value()
    private fun SimpleMeterRegistry.connectionState() = get("xtdb.mysql_source.connection_state").gauge().value()
    private fun SimpleMeterRegistry.lastEventTime() = get("xtdb.mysql_source.last_event_time").gauge().value()
    private fun SimpleMeterRegistry.commits() = get("xtdb.mysql_source.commits.total").counter().count()

    @Test
    fun `GTID lag counts what the upstream has executed past the applied position`() = runTest {
        val reg = SimpleMeterRegistry()
        var executed = position(5)
        val stream = StubStream(ArrayDeque(listOf(MySqlDriver.Transaction.Committed(Gtid(upstream, 3), Instant.EPOCH, emptyList(), position(3)))))

        openSource(reg, StubDriver(ArrayDeque(listOf(stream))) { executed }).use { source ->
            val assignment = launch { source.onPartitionAssigned(0, resumeToken(position(2)), StubIndexer) }
            stream.parked.await()

            assertEquals(2.0, reg.gtidLag(), "applied through 3 of 5")

            executed = position(3)
            assertEquals(0.0, reg.gtidLag(), "caught up")

            assignment.cancelAndJoin()
        }
    }

    @Test
    fun `NaN when the upstream can't be read, and while another node holds the partition`() = runTest {
        val reg = SimpleMeterRegistry()
        var refuse = false
        var queried = false
        val stream = StubStream()

        val driver = StubDriver(ArrayDeque(listOf(stream))) {
            queried = true
            if (refuse) error("connection refused") else position(2)
        }

        openSource(reg, driver).use { source ->
            assertTrue(reg.gtidLag().isNaN(), "unassigned")
            assertFalse(queried, "an unassigned source must not connect to the upstream")

            val assignment = launch { source.onPartitionAssigned(0, resumeToken(position(2)), StubIndexer) }
            stream.parked.await()
            assertEquals(0.0, reg.gtidLag())

            refuse = true
            assertTrue(reg.gtidLag().isNaN(), "failed read — unknown, not caught up")

            assignment.cancelAndJoin()
            assertTrue(reg.gtidLag().isNaN(), "demoted")
        }
    }

    @Test
    fun `connection state follows whether the stream is connected`() = runTest {
        val reg = SimpleMeterRegistry()
        val stream = StubStream()

        openSource(reg, StubDriver(ArrayDeque(listOf(stream))) { position(0) }).use { source ->
            assertEquals(0.0, reg.connectionState())

            val assignment = launch { source.onPartitionAssigned(0, resumeToken(GtidSet.EMPTY), StubIndexer) }
            stream.parked.await()
            assertEquals(1.0, reg.connectionState())

            stream.connected = false
            assertEquals(0.0, reg.connectionState(), "a stream that has lost its connection")

            assignment.cancelAndJoin()
            assertEquals(0.0, reg.connectionState(), "demoted")
        }
    }

    @Test
    fun `a new assignment doesn't inherit the last one's event time`() = runTest {
        val reg = SimpleMeterRegistry()
        val commitTime = Instant.parse("2026-08-26T09:00:00Z")

        val firstStream = StubStream(ArrayDeque(listOf(MySqlDriver.Transaction.Committed(Gtid(upstream, 1), commitTime, emptyList(), position(1)))))
        val secondStream = StubStream()

        openSource(reg, StubDriver(ArrayDeque(listOf(firstStream, secondStream))) { position(1) }).use { source ->
            assertEquals(0.0, reg.lastEventTime(), "no events before the first assignment")

            val first = launch { source.onPartitionAssigned(0, resumeToken(GtidSet.EMPTY), StubIndexer) }
            firstStream.parked.await()
            assertEquals(commitTime.epochSecond.toDouble(), reg.lastEventTime(), "the applied commit's time")
            assertEquals(1.0, reg.commits())
            first.cancelAndJoin()

            val second = launch { source.onPartitionAssigned(0, resumeToken(position(1)), StubIndexer) }
            secondStream.parked.await()
            assertEquals(0.0, reg.lastEventTime(), "this assignment has applied nothing of its own")

            second.cancelAndJoin()
        }
    }
}
