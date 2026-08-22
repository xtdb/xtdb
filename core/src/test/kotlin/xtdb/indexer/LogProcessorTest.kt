package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.api.IndexerConfig
import org.junit.jupiter.api.Timeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.api.log.*
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.InstantSource
import java.util.concurrent.TimeUnit

@Timeout(10, unit = TimeUnit.SECONDS)
class LogProcessorTest {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
        nodeBase.close()
    }

    private fun mockBufferPool(epoch: Int = 0) =
        mockk<BufferPool>(relaxed = true) { every { this@mockk.epoch } returns epoch }

    private fun newPartitionState(name: String = "test-db", liveIndex: LiveIndex = mockk(relaxed = true)) =
        PartitionState(
            BlockCatalog(null),
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )

    private class Fixture(
        val sourceLog: Log<SourceMessage>,
        val replicaLog: Log<ReplicaMessage>,
        bufferPoolEpoch: Int = 0,
        liveIndex: LiveIndex,
        val test: LogProcessorTest,
        backgroundScope: CoroutineScope,
    ) : AutoCloseable {
        val partitionState = PartitionState(
            BlockCatalog(null),
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )
        val bufferPool = test.mockBufferPool(bufferPoolEpoch)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader =
            BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())

        val logProc = LogProcessor(
            test.allocator, test.nodeBase, mockk(relaxed = true),
            partitionStorage, partitionState, "test-db", watchers, blockUploader,
            mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
            externalSourceFactory = null,
            scope = scope,
            mayLead = true,
            electionConfig = replicaLog.electionConfig,
            flushTimeout = IndexerConfig().flushDuration,
        )

        override fun close() {
            runBlocking { scope.coroutineContext.job.cancelAndJoin() }
            logProc.close()
            sourceLog.close()
            replicaLog.close()
        }
    }

    private fun CoroutineScope.fixture(
        epoch: Int = 0,
        liveIndex: LiveIndex = mockk(relaxed = true),
        replicaLog: Log<ReplicaMessage> = InMemoryLog(InstantSource.system(), epoch),
    ) = Fixture(
        InMemoryLog(InstantSource.system(), epoch), replicaLog,
        bufferPoolEpoch = epoch, liveIndex = liveIndex, test = this@LogProcessorTest, backgroundScope = this,
    )

    // A source message is only processed by an elected leader tailing the source log, so awaiting its
    // watermark is awaiting the whole path: quiet observed, claim appended, claim read back and
    // conferring, follower swapped for a leader, source tail live.
    private suspend fun Fixture.awaitLeaderProcessing() {
        val flush = sourceLog.appendMessage(SourceMessage.FlushBlock(null))
        watchers.awaitSource(flush.msgId)
    }

    @Test
    fun `a fresh node elects itself off a quiet log and serves the source log`() = runTest {
        fixture().use { it.awaitLeaderProcessing() }
    }

    @Test
    fun `a fresh node elects itself with a non-zero epoch`() = runTest {
        fixture(epoch = 1).use { it.awaitLeaderProcessing() }
    }

    @Test
    fun `a starting node replays the replica log as a follower`() = runTest {
        val liveIndex = mockk<LiveIndex>(relaxed = true) { every { latestCompletedTx } returns null }

        fixture(liveIndex = liveIndex).use { fixture ->
            fixture.replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, java.time.Instant.now(), true, null, emptyMap())
            )

            fixture.watchers.awaitTx(1)
            verify { liveIndex.commitTx(any(), any()) }
        }
    }

    @Test
    fun `a claim lands one term above everything already on the log`() = runTest {
        fixture().use { fixture ->
            // a previous leader reached 0.9; whatever elects next must claim above it
            fixture.replicaLog.appendMessage(ReplicaMessage.NoOp(termId = LeaderTerm.of(0, 9)))

            fixture.awaitLeaderProcessing()

            assertEquals(
                LeaderTerm.of(0, 10), fixture.partitionState.termFence.highest,
                "the claim is adjudicated against a prefix that cannot already contain its term"
            )
        }
    }

    /**
     * Interleaves a rival's claim just ahead of this node's own first claim — the same-term collision a
     * single node cannot otherwise be made to lose deterministically.
     */
    private class RivalGetsInFirst(private val delegate: Log<ReplicaMessage>) : Log<ReplicaMessage> by delegate {
        var rivalPending = true

        override suspend fun appendMessage(message: ReplicaMessage, partition: Int): Log.MessageMetadata {
            if (rivalPending && message is ReplicaMessage.NoOp && message.termId != LeaderTerm.NONE) {
                rivalPending = false
                delegate.appendMessage(ReplicaMessage.NoOp(termId = message.termId), partition)
            }
            return delegate.appendMessage(message, partition)
        }
    }

    @Test
    fun `a lost claim returns the node to following, and it claims again above the winner`() = runTest {
        val replicaLog = RivalGetsInFirst(InMemoryLog(InstantSource.system(), 0))

        fixture(replicaLog = replicaLog).use { fixture ->
            // the earlier of two same-term claims wins, so the rival's copy defeats ours — and losing
            // must leave the database serving, with a later claim (above the rival's term) succeeding
            fixture.awaitLeaderProcessing()

            assertTrue(
                fixture.partitionState.termFence.highest >= LeaderTerm.of(0, 2),
                "the winning claim sits above the term the rival took"
            )
        }
    }

    @Test
    fun `a higher term read back resigns the leader cleanly, and it stands again`() = runTest {
        fixture().use { fixture ->
            fixture.awaitLeaderProcessing()
            val termLed = fixture.partitionState.termFence.highest

            // another node's claim confers: this leader reads it back and resigns without failing
            // the database...
            fixture.replicaLog.appendMessage(ReplicaMessage.NoOp(termId = termLed + 1))

            // ...and, the claimant never following through, this node times out and claims above it.
            fixture.awaitLeaderProcessing()

            assertTrue(
                fixture.partitionState.termFence.highest > termLed + 1,
                "the re-claim sits above the term that superseded us — the fence survived the resignation"
            )
        }
    }

    @Test
    fun `a node that may not lead never claims`() = runTest {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val partitionState = newPartitionState()
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), mockBufferPool(), null)
        val blockUploader =
            BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, backgroundScope)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val scope = CoroutineScope(SupervisorJob())
        val logProc = LogProcessor(
            allocator, nodeBase, mockk(relaxed = true),
            partitionStorage, partitionState, "test-db", watchers, blockUploader,
            mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
            externalSourceFactory = null,
            scope = scope,
            mayLead = false,
            electionConfig = replicaLog.electionConfig,
            flushTimeout = IndexerConfig().flushDuration,
        )

        // several times the election-timeout maximum, so a claim that was going to happen has
        withContext(Dispatchers.Default) { delay(1000) }

        assertEquals(
            -1L, replicaLog.latestSubmittedOffset(),
            "an ineligible node appends nothing, however quiet its log"
        )

        scope.coroutineContext.job.cancelAndJoin()
        logProc.close()
        sourceLog.close()
        replicaLog.close()
    }
}
