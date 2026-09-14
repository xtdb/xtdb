package xtdb.postgres

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.tx.BlockDetails
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.postgres.proto.postgresSourceToken
import java.time.Instant
import kotlin.test.assertIs

/**
 * What the source does when its poll loop ends (#5878).
 *
 * The stubs are this class's own: the driver records the LSN each stream was asked to resume from, and the
 * streams end on cue, so a test decides what happens at the moment the loop stops.
 */
class PostgresSourceReconnectTest {

    /**
     * Cancels the assignment from inside an idle poll, and returns rather than throwing — so the poll loop
     * leaves through its own condition, which is the only exit no exception passes through.
     */
    private class StandDownStream(private val standDown: Job) : PostgresDriver.ChangeStream {
        override val walEnd get() = 0L
        override suspend fun acknowledge(lsn: Long) = Unit
        override fun close() = Unit

        override suspend fun poll(): PostgresDriver.Transaction? {
            standDown.cancel()
            return null
        }
    }

    /** Hands out [streams] in order, recording the LSN each was asked to resume from. */
    private class RecordingDriver(private val streams: ArrayDeque<PostgresDriver.ChangeStream>) : PostgresDriver {
        val startLsns = mutableListOf<Long>()

        override fun openSnapshot(): PostgresDriver.SnapshotReader = error("resumes, so never snapshots")

        override suspend fun openStream(startLsn: Long): PostgresDriver.ChangeStream {
            startLsns += startLsn
            return streams.removeFirst()
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

    // snapshotCompleted, so the assignment resumes straight into streaming
    private val resumeToken = postgresSourceToken {
        latestCommittedLsn = 0
        snapshotCompleted = true
    }.toByteArray()

    private fun openSource(driver: PostgresDriver) = PostgresSource("xtdb", driver, "test_slot", DirectMirror())

    @Test
    fun `a stand-down between polls cancels rather than reopening`() = runTest {
        val standDown = Job()
        val outcome = CompletableDeferred<Throwable?>()

        val driver = RecordingDriver(ArrayDeque(listOf(StandDownStream(standDown))))

        openSource(driver).use { source ->
            launch(standDown) {
                outcome.complete(
                    runCatching { source.onPartitionAssigned(0, resumeToken, StubIndexer) }.exceptionOrNull()
                )
            }

            assertIs<CancellationException>(outcome.await())
            assertEquals(listOf(0L), driver.startLsns, "and does not reopen")
        }
    }
}
