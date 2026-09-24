package xtdb.postgres

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.postgresql.util.PSQLException
import org.postgresql.util.PSQLState
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.tx.BlockDetails
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.postgres.proto.postgresSourceToken
import java.io.EOFException
import java.net.SocketException
import java.time.Instant
import java.time.InstantSource
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.minutes

/**
 * What the source does when the replication connection dies under a running stream (#5878).
 *
 * The stubs are this class's own rather than shared with [PostgresSourceMetricsTest]: that one needs a
 * stream which parks, this one needs streams which die on cue and a driver which records where each was
 * asked to resume from.
 */
class PostgresSourceReconnectTest {

    // `runTest` runs on virtual time, so this costs nothing to run and only has to clear the reconnect
    // backoff — it is here to turn a stream that is never reached into a failure rather than a hang.
    private val BACKOFF_HEADROOM = 10.minutes

    private fun tx(lsn: Long) = PostgresDriver.Transaction(lsn, Instant.EPOCH, emptyList())

    /** A clock the test moves by hand, so a stream's lifetime is decided rather than waited out. */
    private class TestClock : InstantSource {
        private var now: Instant = Instant.EPOCH

        override fun instant() = now
        fun advance(millis: Long) { now = now.plusMillis(millis) }
    }

    /** [n] streams that die the moment they are polled, without delivering anything. */
    private fun fruitlessStreams(n: Int) = List(n) { DyingStream(ArrayDeque()) { throw connectionLost() } }

    /** A connection failure in the shape pgjdbc raises one: `08006` over whatever broke underneath. */
    private fun connectionLost(cause: Throwable = SocketException("Broken pipe")) =
        PSQLException("Database connection failed when reading from copy", PSQLState.CONNECTION_FAILURE, cause)

    /** Yields [txs], then dies the way a killed WAL sender does. */
    private class DyingStream(private val txs: ArrayDeque<PostgresDriver.Transaction>, private val die: () -> Nothing) :
        PostgresDriver.ChangeStream {

        override val walEnd get() = 0L
        override suspend fun acknowledge(lsn: Long) = Unit
        override suspend fun poll() = txs.removeFirstOrNull() ?: die()
        override fun close() = Unit
    }

    /**
     * Cancels the assignment from inside an idle poll, and returns rather than throwing — so the poll loop
     * leaves through its own condition, which is the only exit no exception passes through.
     */
    private class StandDownStream(private val standDown: CoroutineScope) : PostgresDriver.ChangeStream {
        override val walEnd get() = 0L
        override suspend fun acknowledge(lsn: Long) = Unit
        override fun close() = Unit

        override suspend fun poll(): PostgresDriver.Transaction? {
            standDown.cancel()
            return null
        }
    }

    /** Parks once it has nothing left, so a test can wait for the stream to be reached. */
    private class ParkedStream : PostgresDriver.ChangeStream {
        val parked = CompletableDeferred<Unit>()

        override val walEnd get() = 0L
        override suspend fun acknowledge(lsn: Long) = Unit

        override suspend fun poll(): PostgresDriver.Transaction? {
            parked.complete(Unit)
            awaitCancellation()
        }

        override fun close() = Unit
    }

    /** Hands out [streams] in order, recording the LSN each was asked to resume from. */
    private class RecordingDriver(private val streams: ArrayDeque<PostgresDriver.ChangeStream>) : PostgresDriver {
        val startLsns = mutableListOf<Long>()

        override fun openSnapshot(): PostgresDriver.SnapshotReader = error("resumes, so never snapshots")

        override suspend fun openStream(startLsn: Long): PostgresDriver.ChangeStream {
            startLsns += startLsn
            return streams.removeFirstOrNull() ?: error("opened more streams than this test provided: $startLsns")
        }

        override fun publicationExists() = true
        override fun queryWalLagBytes(): Long = 0
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

    /** Hands back [handle] for every submitted tx, so a test decides when — and whether — it applies. */
    private class PendingIndexer(private val handle: Deferred<TransactionResult>) : TxIndexer {
        override val latestBlock = MutableStateFlow<BlockDetails?>(null)

        override suspend fun executeTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
            writer: suspend (OpenTx) -> TxIndexer.TxResult,
        ): TransactionResult = handle.await()

        override suspend fun submitTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?,
            writer: suspend (OpenTx) -> TxIndexer.TxResult,
        ): Deferred<TransactionResult> = handle
    }

    // snapshotCompleted, so the assignment resumes straight into streaming
    private val resumeToken = postgresSourceToken {
        latestCommittedLsn = 0
        snapshotCompleted = true
    }.toByteArray()

    private fun openSource(driver: PostgresDriver, instantSource: InstantSource = InstantSource.system()) =
        PostgresSource("xtdb", driver, "test_slot", DirectMirror(), instantSource = instantSource)

    @Test
    fun `a mid-stream connection death resumes from the furthest LSN submitted`() = runTest {
        val resumed = ParkedStream()
        val driver = RecordingDriver(
            ArrayDeque(listOf(DyingStream(ArrayDeque(listOf(tx(10), tx(20)))) { throw connectionLost() }, resumed))
        )

        openSource(driver).use { source ->
            val assignment = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }

            withTimeout(BACKOFF_HEADROOM) { resumed.parked.await() }
            assertEquals(listOf(0L, 20L), driver.startLsns)

            assignment.cancelAndJoin()
        }
    }

    @Test
    fun `an ingest failure ends the assignment rather than reconnecting`() = runTest {
        // still pending when the connection dies, so only a drain that awaits it finds the failure
        val handle = CompletableDeferred<TransactionResult>()

        val driver = RecordingDriver(
            ArrayDeque(
                listOf(
                    DyingStream(ArrayDeque(listOf(tx(10)))) {
                        handle.completeExceptionally(IllegalStateException("indexer exploded"))
                        throw connectionLost()
                    }
                )
            )
        )

        openSource(driver).use { source ->
            val thrown = assertFailsWith<IllegalStateException> {
                source.onPartitionAssigned(0, resumeToken, PendingIndexer(handle))
            }

            assertEquals("indexer exploded", thrown.message)
            assertEquals(listOf(0L), driver.startLsns, "the connection is not reopened")
        }
    }

    @Test
    fun `a connection that never recovers gives up rather than reconnecting forever`() = runTest {
        val driver = RecordingDriver(ArrayDeque(fruitlessStreams(RECONNECT_MAX_ATTEMPTS + 1)))

        openSource(driver).use { source ->
            assertFailsWith<PSQLException> { source.onPartitionAssigned(0, resumeToken, StubIndexer) }

            assertEquals(
                List(RECONNECT_MAX_ATTEMPTS + 1) { 0L }, driver.startLsns,
                "one open, then one per attempt, none of them past the resume position",
            )
        }
    }

    @Test
    fun `a connection that delivers transactions starts the attempt count again`() = runTest {
        val survivor = ParkedStream()

        // more fruitless attempts in total than the bound allows, either side of one that makes progress
        val streams =
            fruitlessStreams(RECONNECT_MAX_ATTEMPTS - 1) +
                DyingStream(ArrayDeque(listOf(tx(10)))) { throw connectionLost() } +
                fruitlessStreams(RECONNECT_MAX_ATTEMPTS - 1) +
                survivor

        val driver = RecordingDriver(ArrayDeque(streams))

        openSource(driver).use { source ->
            val assignment = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }

            withTimeout(BACKOFF_HEADROOM) { survivor.parked.await() }

            // Without the reset the count would reach the bound on the first fruitless attempt after the
            // progress, so the second run of opens would stop one in rather than completing.
            assertEquals(
                List(RECONNECT_MAX_ATTEMPTS) { 0L } + List(RECONNECT_MAX_ATTEMPTS) { 10L }, driver.startLsns,
                "a run of attempts at the resume position, then a whole run more at the tx that advanced it",
            )

            assignment.cancelAndJoin()
        }
    }

    /**
     * Runs an assignment whose stream cancels it from inside the poll that then fails, so the failure is
     * classified against a coroutine already standing down. The interleaving is forced rather than raced,
     * and the outcome is reported out through a deferred that isn't the cancelled coroutine's child.
     */
    private suspend fun CoroutineScope.standDownRacing(failure: () -> Nothing): Pair<Throwable?, List<Long>> {
        val standDown = CoroutineScope(currentCoroutineContext() + Job())
        val outcome = CompletableDeferred<Throwable?>()

        val driver = RecordingDriver(ArrayDeque(listOf(DyingStream(ArrayDeque()) { standDown.cancel(); failure() })))

        openSource(driver).use { source ->
            standDown.launch {
                outcome.complete(
                    runCatching { source.onPartitionAssigned(0, resumeToken, StubIndexer) }.exceptionOrNull()
                )
            }

            return outcome.await() to driver.startLsns
        }
    }

    @Test
    fun `a stand-down racing a lost connection cancels rather than returning`() = runTest {
        val (thrown, startLsns) = standDownRacing { throw connectionLost() }

        assertIs<CancellationException>(thrown)
        assertEquals(listOf(0L), startLsns, "and does not reopen")
    }

    @Test
    fun `a stand-down racing a lost connection cancels rather than failing the database`() = runTest {
        // pgjdbc wraps whatever broke on the copy stream. An EOFException is an IOException but not a
        // SocketException, so a connection failure carrying one leaves by a different path.
        val (thrown, startLsns) = standDownRacing { throw connectionLost(EOFException("unexpected end of stream")) }

        assertIs<CancellationException>(thrown)
        assertEquals(listOf(0L), startLsns, "and does not reopen")
    }

    @Test
    fun `a stand-down between polls cancels rather than reopening`() = runTest {
        val standDown = CoroutineScope(currentCoroutineContext() + Job())
        val outcome = CompletableDeferred<Throwable?>()

        val driver = RecordingDriver(ArrayDeque(listOf(StandDownStream(standDown))))

        openSource(driver).use { source ->
            standDown.launch {
                outcome.complete(
                    runCatching { source.onPartitionAssigned(0, resumeToken, StubIndexer) }.exceptionOrNull()
                )
            }

            assertIs<CancellationException>(outcome.await())
            assertEquals(listOf(0L), driver.startLsns, "and does not reopen")
        }
    }

    @Test
    fun `a connection that stays up does not accumulate attempts`() = runTest {
        val clock = TestClock()
        val survivor = ParkedStream()

        // None of these delivers anything, so only their lifetime distinguishes them from a flap — and there
        // are more of them than the bound allows
        val streams = List(RECONNECT_MAX_ATTEMPTS + 1) {
            DyingStream(ArrayDeque()) {
                clock.advance(RECONNECT_HEALTHY_STREAM_MS)
                throw connectionLost()
            }
        } + survivor

        val driver = RecordingDriver(ArrayDeque(streams))

        openSource(driver, clock).use { source ->
            val assignment = launch { source.onPartitionAssigned(0, resumeToken, StubIndexer) }

            withTimeout(BACKOFF_HEADROOM) { survivor.parked.await() }
            assertEquals(
                RECONNECT_MAX_ATTEMPTS + 2, driver.startLsns.size,
                "one open per stream, the bound never reached",
            )

            assignment.cancelAndJoin()
        }
    }
}
