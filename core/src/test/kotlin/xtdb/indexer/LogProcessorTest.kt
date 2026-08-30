package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Timeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.api.IndexerConfig
import xtdb.api.log.*
import xtdb.block.proto.block
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.BufferPool
import java.time.Instant
import java.time.InstantSource
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

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

    private fun newPartitionState(
        liveIndex: LiveIndex = mockk(relaxed = true),
        boundaryTermId: Long? = null,
    ) = PartitionState(
        boundaryTermId
            ?.let { TableCatalog(mockBufferPool(), block { blockIndex = 0; termId = it }) }
            ?: TableCatalog(mockBufferPool()),
        createTrieCatalog(),
        liveIndex
    )

    /**
     * One node's worth of the fixture, torn down as a unit.
     *
     * Every case here drives a real [LogProcessor] against real in-memory logs, because the election is
     * only observable in what reaches those logs and in whether the node ends up leading.
     */
    private inner class TestNode(
        val sourceLog: InMemoryLog<SourceMessage>,
        val replicaLog: InMemoryLog<ReplicaMessage>,
        boundaryTermId: Long? = null,
        liveIndex: LiveIndex = mockk(relaxed = true),
        readOnly: Boolean = false,
        // A quarter of the in-process scale, so a case turning on an empty poll settles within
        // awaitLeadership's budget. The 5-10x election range comes off this, as in production.
        electionDriver: ElectionDriver = RealElectionDriver(assertInterval = 25.milliseconds),
    ) : AutoCloseable {
        val partitionState = newPartitionState(liveIndex = liveIndex, boundaryTermId = boundaryTermId)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), mockBufferPool(), null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)
        val scope = CoroutineScope(SupervisorJob())

        val logProc = LogProcessor(
            allocator, nodeBase, mockk(relaxed = true),
            partitionStorage, partitionState, "test-db", watchers,
            BlockUploader(partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null, scope),
            mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
            externalSource = null,
            scope = scope,
            flushTimeout = IndexerConfig().flushDuration,
            electionDriver = electionDriver,
            readOnly = readOnly,
        )

        override fun close() {
            runBlocking { scope.coroutineContext.job.cancelAndJoin() }
            logProc.close()
        }
    }

    private fun freshLogs() =
        InMemoryLog<SourceMessage>(InstantSource.system(), 0) to InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)

    /**
     * An election timeout no test reaches, leaving a claim taken before reading as the only one available.
     *
     * For a case about what a node does with the log it starts against, where a second election arriving
     * part-way would decide the outcome instead.
     */
    private fun noElectionTimeout() = RealElectionDriver(assertInterval = 2.minutes)

    /** Polls the node's own view rather than the clock: every case here settles in a bounded number of steps. */
    private suspend fun awaitLeadership(node: TestNode, expected: Boolean) =
        withContext(Dispatchers.Default) {
            withTimeout(5_000) { while (node.logProc.isLeader != expected) yield() }
        }

    @Test
    fun `a node claims without reading when the log is empty and no block has been written`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
            awaitLeadership(node, expected = true)

            assertEquals(
                1L, node.logProc.termFence.highestSeen,
                "the first term on a log nobody has led is 1"
            )
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a node that may not lead never claims`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
            val seeded = replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1)
            )

            // Applying a record proves the reader has been round its loop, so it has had every chance to
            // claim that an eligible node would have taken before its first poll.
            node.watchers.awaitReplicaMsg(seeded.msgId)

            assertFalse(node.logProc.isLeader)
            assertEquals(
                seeded.logOffset, replicaLog.latestSubmittedOffset(),
                "nothing of ours reached the log"
            )
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a claim tying a term already on the log confers nothing`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        val seeded = replicaLog.appendMessage(
            ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = 1)
        )

        TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
            // The fence starts unset, so the node claims before its first read and the claim lands behind
            // the seeded record — where it ties term 1 rather than exceeding it.
            val claim = withContext(Dispatchers.Default) {
                withTimeout(5_000) {
                    while (replicaLog.latestSubmittedOffset() == seeded.logOffset) yield()
                    replicaLog.latestSubmittedMsgId()
                }
            }

            node.watchers.awaitReplicaMsg(claim)
            assertFalse(node.logProc.isLeader)
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `two nodes claiming the same fresh log settle on one leader`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        TestNode(sourceLog, replicaLog).use { a ->
            TestNode(sourceLog, replicaLog).use { b ->
                withContext(Dispatchers.Default) {
                    withTimeout(5_000) {
                        while (!a.logProc.isLeader && !b.logProc.isLeader) yield()

                        // Position decides the claim, so the loser's confers nothing; and the winner
                        // asserts often enough that the loser's polls never come back empty for it to
                        // try again.
                        repeat(200) {
                            assertFalse(
                                a.logProc.isLeader && b.logProc.isLeader,
                                "two nodes never lead one database at the same time"
                            )
                            delay(5)
                        }
                    }
                }
            }
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a leader superseded by a higher term stands down and follows`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
            awaitLeadership(node, expected = true)

            replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2L))

            awaitLeadership(node, expected = false)
            assertEquals(2L, node.logProc.termFence.highestSeen)
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `the fence seeds from the persisted block boundary and only rises`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        TestNode(sourceLog, replicaLog, boundaryTermId = 9L).use { node ->
            assertEquals(
                9L, node.logProc.termFence.highestSeen,
                "a node that has flushed a block starts from the term that cut it"
            )

            // So it cannot claim on sight, and when it does claim it claims above the boundary.
            awaitLeadership(node, expected = true)
            assertEquals(10L, node.logProc.termFence.highestSeen)
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `the reader discards a fenced record, still advancing the consume position`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()

        TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
            val leader = replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = 2L)
            )
            val superseded = replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), srcMsgId = 2, termId = 1L)
            )

            node.watchers.awaitReplicaMsg(superseded.msgId)

            assertEquals(1L, node.watchers.latestTxId, "the superseded leader's tx was never applied")
            assertEquals(2L, node.logProc.termFence.highestSeen)
            assertTrue(leader.msgId < superseded.msgId)
        }

        sourceLog.close()
        replicaLog.close()
    }

    @Test
    fun `a node applies the messages already on the log, then claims`() = runTest {
        val (sourceLog, replicaLog) = freshLogs()
        val liveIndex = mockk<LiveIndex>(relaxed = true) { every { latestCompletedTx } returns null }

        replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap()))

        TestNode(sourceLog, replicaLog, liveIndex = liveIndex).use { node ->
            node.watchers.awaitTx(1)

            awaitLeadership(node, expected = true)
        }

        sourceLog.close()
        replicaLog.close()
    }
}
