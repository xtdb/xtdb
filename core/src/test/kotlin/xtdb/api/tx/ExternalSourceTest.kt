package xtdb.api.tx

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.log.*
import xtdb.api.error.Incorrect
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.catalog.TableCatalog
import xtdb.database.Database
import xtdb.database.DatabaseLogs
import xtdb.database.DatabasePartition
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.indexer.LeaderTermTest
import xtdb.indexer.LiveIndex
import xtdb.indexer.LogProcessor.LogsDriver
import xtdb.storage.MemoryStorage
import xtdb.tx.TxOpts
import java.time.InstantSource
import java.time.ZoneId
import kotlin.time.Duration.Companion.milliseconds

internal class ExternalSourceTest : LeaderTermTest() {

    private lateinit var bufferPool: MemoryStorage
    private lateinit var liveIndex: LiveIndex

    @BeforeEach
    fun openRealIndex() {
        bufferPool = MemoryStorage(allocator, 0)
        liveIndex = LiveIndex.open(allocator, TableCatalog(bufferPool), createTrieCatalog())
    }

    @AfterEach
    fun closeRealIndex() {
        liveIndex.close()
        bufferPool.close()
    }

    /**
     * Simple in-memory ExternalSource for testing.
     * Send signals to [channel]; each signal submits a tx via [index] (by default the blocking
     * [xtdb.api.tx.TxIndexer.executeTx]; pass a `submit`-based [index] to drive the fire-and-forget path).
     */
    class InMemoryExternalSource(
        val channel: Channel<ExternalSourceToken?> = Channel(100),
        private val index: suspend TxIndexer.(ExternalSourceToken?) -> Unit = {
            executeTx(it) { TxResult.Committed() }
        },
    ) : ExternalSource {

        override suspend fun onPartitionAssigned(
            partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
        ) {
            for (token in channel) {
                txIndexer.index(token)
            }
        }

        override fun close() {
        }
    }

    private class ExtTerm(val watchers: Watchers, private val replicaLog: InMemoryLog<ReplicaMessage>) {

        fun resolvedTxs() =
            replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
                .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()

        /** Supersede the term, by putting a higher term on the log for it to read back. */
        suspend fun supersede() = replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2))

        /**
         * Await the term's failure. `awaitTx` returns when a tx applies and throws when the database
         * fails, so it is a handle on a term that is going to die — where reading [Watchers.exception]
         * needs a sleep first.
         */
        suspend fun awaitFailure(): Throwable {
            val failure =
                try {
                    watchers.awaitTx(0)
                    null
                } catch (e: Throwable) {
                    e
                }

            return failure ?: fail("the term applied a tx rather than failing")
        }
    }

    private fun TestScope.extTerm(
        extSource: ExternalSource,
        liveIndex: LiveIndex = this@ExternalSourceTest.liveIndex,
        wrapDriver: (LogsDriver) -> LogsDriver = { it },
    ): ExtTerm {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog, bufferPool = bufferPool, liveIndex = liveIndex,
            watchers = watchers, extSource = extSource, wrapDriver = wrapDriver,
        )

        return ExtTerm(watchers, replicaLog)
    }

    @Test
    fun `execute appends ResolvedTx to replica log`() = runTest {
        val extSource = InMemoryExternalSource()
        val term = extTerm(extSource)

        extSource.channel.send(null)
        term.watchers.awaitTx(0)

        val resolved = term.resolvedTxs().single()
        assertTrue(resolved.committed)
        assertEquals(0L, resolved.txId)
        assertEquals(
            -1L, resolved.srcMsgId,
            "ext-source ResolvedTx carries the leader's source-log watermark (-1 — no source-log records yet)"
        )
    }

    @Test
    fun `successive external events appear in the replica log with monotonic txIds`() = runTest {
        val extSource = InMemoryExternalSource()
        val term = extTerm(extSource)

        extSource.channel.send(null)
        extSource.channel.send(null)
        term.watchers.awaitTx(1)

        val resolvedTxs = term.resolvedTxs()
        assertEquals(listOf(0L, 1L), resolvedTxs.map { it.txId })
        assertTrue(resolvedTxs.all { it.committed }, "both txs should be committed")
    }

    @Test
    fun `execute threads resumeToken to watchers`() = runTest {
        val extSource = InMemoryExternalSource()
        val term = extTerm(extSource)

        val token = "kafka-offset:42".toByteArray()
        extSource.channel.send(token)
        term.watchers.awaitTx(0)

        assertArrayEquals(token, term.watchers.externalSourceToken)
    }

    @Test
    fun `error in external source propagates to watchers`() = runTest {
        val failingSource = object : ExternalSource {
            override suspend fun onPartitionAssigned(
                partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
            ) {
                throw RuntimeException("source poll failed")
            }

            override fun close() {}
        }

        val failure = extTerm(failingSource).awaitFailure()
        assertTrue(
            failure.message?.contains("source poll failed") == true,
            "the source's own failure reaches the watchers: $failure"
        )
    }

    @Test
    fun `fault in the commit pipeline tips watchers into Failed`() = runTest {
        val extSource = InMemoryExternalSource()
        val term = extTerm(extSource, liveIndex = faultingLiveIndex())

        extSource.channel.send(null)

        val failure = term.awaitFailure()
        assertTrue(
            failure.message?.contains("commit pipeline fault") == true,
            "an apply fault fails the database rather than only the tx: $failure"
        )
    }

    @Test
    fun `submit applies txs fire-and-forget with monotonic txIds`() = runTest {
        val extSource = InMemoryExternalSource(index = { submitTx(it) { TxResult.Committed() } })
        val term = extTerm(extSource)

        extSource.channel.send(null)
        extSource.channel.send(null)
        term.watchers.awaitTx(1)

        val resolvedTxs = term.resolvedTxs()
        assertEquals(listOf(0L, 1L), resolvedTxs.map { it.txId })
        assertTrue(resolvedTxs.all { it.committed }, "both fire-and-forget txs should commit")
    }

    @Test
    fun `a superseded term stands the source down without failing the database`() = runTest {
        val stoodDown = CompletableDeferred<Unit>()
        val source = object : ExternalSource {
            override suspend fun onPartitionAssigned(
                partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
            ) {
                try {
                    while (true) {
                        txIndexer.submitTx(null) { TxResult.Committed() }
                        delay(10.milliseconds)
                    }
                } finally {
                    stoodDown.complete(Unit)
                }
            }

            override fun close() {}
        }

        val term = extTerm(source)
        term.supersede()

        stoodDown.await()
        assertNull(term.watchers.exception, "a resignation leaves the database queryable")
    }

    @Test
    fun `executeTx throws when the drain faults, rather than hanging`() = runTest {
        val thrown = CompletableDeferred<Throwable>()
        val extSource = object : ExternalSource {
            override suspend fun onPartitionAssigned(
                partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
            ) {
                try {
                    txIndexer.executeTx(null) { TxResult.Committed() }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    thrown.complete(e)
                }
            }

            override fun close() {}
        }

        extTerm(extSource, liveIndex = faultingLiveIndex())

        val e = thrown.await()
        assertEquals(
            "commit pipeline fault", e.message,
            "executeTx surfaces the drain fault instead of hanging"
        )
    }

    @Test
    fun `a replica-log append fault fails the term rather than wedging it`() = runTest {
        val failingDriver = { inner: LogsDriver ->
            object : LogsDriver by inner {
                override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
                    throw RuntimeException("replica-log append fault")
            }
        }

        val extSource = InMemoryExternalSource()
        val term = extTerm(extSource, wrapDriver = failingDriver)

        extSource.channel.send(null)

        val failure = term.awaitFailure()
        assertTrue(
            failure.message?.contains("replica-log append fault") == true,
            "the append fault fails the term, rather than leaving it wedged: $failure"
        )
    }

    @Test
    fun `submitTxBlocking rejects when externalSource is configured`() {
        val extFactory = mockk<ExternalSource.Factory>()
        val config = Database.Config(externalSource = extFactory)

        // note: not .use — Database.close() would close `allocator`, which @AfterEach also closes
        val partition = DatabasePartition(
            storage = PartitionStorage(DatabaseLogs(null, null), null, null),
            state = PartitionState(null, null, null),
            watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1),
        )
        val db = Database(
            allocator = allocator,
            config = config,
            name = "cdc",
            logs = DatabaseLogs(null, null),
            isIndexing = false,
            partitions = listOf(partition),
            meterRegistry = null,
        )

        val ex = assertThrows(Incorrect::class.java) {
            db.submitTxBlocking(emptyList(), TxOpts(defaultTz = ZoneId.of("UTC")))
        }
        assertTrue(ex.message!!.contains("external source"), "message mentions external source")
    }

    private fun faultingLiveIndex() = mockk<LiveIndex>(relaxed = true) {
        every { commitTx(any(), any()) } throws RuntimeException("commit pipeline fault")
    }
}
