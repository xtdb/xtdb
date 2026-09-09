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
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Timeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.api.IndexerConfig
import xtdb.api.log.*
import xtdb.block.proto.block
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.storage.Storage
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.BufferPool
import xtdb.types.MessageId
import xtdb.util.closeAll
import java.io.IOException
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

    // Each test cancel-and-joins its own scope and closes its processor, so everything here is quiescent by teardown — and freed before `allocator`, which the live indexes are children of.
    private val partitionStates = mutableListOf<PartitionState>()

    @AfterEach
    fun tearDown() {
        partitionStates.closeAll()
        allocator.close()
        nodeBase.close()
    }

    private fun mockBufferPool(epoch: Int = 0) =
        mockk<BufferPool>(relaxed = true) { every { this@mockk.epoch } returns epoch }

    private fun newPartitionState(boundaryTermId: Long? = null): PartitionState {
        val tableCatalog = boundaryTermId
            ?.let { TableCatalog(mockBufferPool(), block { blockIndex = 0; termId = it }) }
            ?: TableCatalog(mockBufferPool())
        val trieCatalog = createTrieCatalog()

        return PartitionState(tableCatalog, trieCatalog, LiveIndex.open(allocator, tableCatalog, trieCatalog))
            .also { partitionStates += it }
    }

    /** One node's worth of the fixture, torn down as a unit. */
    private inner class TestNode(
        val sourceLog: InMemoryLog<SourceMessage>,
        val replicaLog: InMemoryLog<ReplicaMessage>,
        boundaryTermId: Long? = null,
        readOnly: Boolean = false,
        // A quarter of the in-process scale, so a case turning on an empty poll settles within awaitLeadership's budget.
        // The 5-10x election range comes off this, as in production.
        electionDriver: ElectionDriver = RealElectionDriver(assertInterval = 25.milliseconds),
        bufferPool: BufferPool = mockBufferPool(),
        logsDriver: (LogProcessor.LogsDriver) -> LogProcessor.LogsDriver = { it },
    ) : AutoCloseable {
        val partitionState = newPartitionState(boundaryTermId = boundaryTermId)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val scope = CoroutineScope(SupervisorJob())

        val logProc = LogProcessor(
            allocator, nodeBase, mockk(relaxed = true),
            partitionStorage, partitionState, "test-db", watchers,
            mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
            externalSource = null,
            scope = scope,
            flushTimeout = IndexerConfig().flushDuration,
            logsDriver = logsDriver(LogProcessor.RealLogsDriver(partitionStorage)),
            electionDriver = electionDriver,
            readOnly = readOnly,
        )

        override fun close() {
            runBlocking { scope.coroutineContext.job.cancelAndJoin() }
            logProc.close()
        }
    }

    private suspend fun withFreshLogs(
        body: suspend (InMemoryLog<SourceMessage>, InMemoryLog<ReplicaMessage>) -> Unit,
    ) =
        InMemoryLog<SourceMessage>(InstantSource.system(), 0).use { sourceLog ->
            InMemoryLog<ReplicaMessage>(InstantSource.system(), 0).use { replicaLog ->
                body(sourceLog, replicaLog)
            }
        }

    /** An election timeout no test reaches, leaving a claim taken before reading as the only one available. */
    private fun noElectionTimeout() = RealElectionDriver(assertInterval = 2.minutes)

    private suspend fun awaitLeadership(node: TestNode, expected: Boolean) =
        withContext(Dispatchers.Default) {
            withTimeout(5_000) { while (node.logProc.isLeader != expected) yield() }
        }

    private suspend fun awaitFailure(node: TestNode) =
        withContext(Dispatchers.Default) {
            withTimeout(5_000) { while (node.watchers.exception == null) yield() }
        }

    private suspend fun awaitFence(node: TestNode, term: Long) =
        withContext(Dispatchers.Default) {
            withTimeout(5_000) { while (node.logProc.termFence.highestSeen < term) yield() }
        }

    private suspend fun awaitReplicaMsg(node: TestNode, msgId: MessageId) =
        withContext(Dispatchers.Default) {
            withTimeout(5_000) { while (node.logProc.latestReplicaMsgId < msgId) yield() }
        }

    @Test
    fun `a node claims without reading when the log is empty and no block has been written`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
                awaitLeadership(node, expected = true)

                assertEquals(
                    1L, node.logProc.termFence.highestSeen,
                    "the first term on a log nobody has led is 1"
                )
            }
        }
    }

    @Test
    fun `a node that may not lead never claims`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                val seeded = replicaLog.appendMessage(
                    ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = 1)
                )

                // Applying a record proves the reader has been round its loop, so it has had every chance to claim that an eligible node would have taken before its first poll.
                awaitReplicaMsg(node, seeded.msgId)

                assertFalse(node.logProc.isLeader)
                assertEquals(
                    seeded.logOffset, replicaLog.latestSubmittedOffset(),
                    "nothing of ours reached the log"
                )
            }
        }
    }

    @Test
    fun `a claim tying a term already on the log confers nothing`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val seeded = replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = 1)
            )

            TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
                // The fence starts unset, so the node claims before its first read and the claim lands behind the seeded record — where it ties term 1 rather than exceeding it.
                val claim = withContext(Dispatchers.Default) {
                    withTimeout(5_000) {
                        while (replicaLog.latestSubmittedOffset() == seeded.logOffset) yield()
                        replicaLog.latestSubmittedMsgId()
                    }
                }

                awaitReplicaMsg(node, claim)
                assertFalse(node.logProc.isLeader)
            }
        }
    }

    @Test
    fun `two nodes claiming the same fresh log settle on one leader`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog).use { a ->
                TestNode(sourceLog, replicaLog).use { b ->
                    withContext(Dispatchers.Default) {
                        withTimeout(5_000) {
                            while (!a.logProc.isLeader && !b.logProc.isLeader) yield()

                            // The winner asserts often enough that the loser's polls never come back empty for it to try again.
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
        }
    }

    @Test
    fun `a leader superseded by a higher term stands down and follows`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
                awaitLeadership(node, expected = true)

                val superseding = replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2L))

                awaitLeadership(node, expected = false)

                // The term resigns before folding, so the record is re-offered to the follower that replaces it — and the position advances only once that follower has folded it.
                awaitReplicaMsg(node, superseding.msgId)
                assertEquals(2L, node.logProc.termFence.highestSeen)
            }
        }
    }

    @Test
    fun `the fence seeds from the persisted block boundary and only rises`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, boundaryTermId = 9L).use { node ->
                assertEquals(
                    9L, node.logProc.termFence.highestSeen,
                    "a node that has flushed a block starts from the term that cut it"
                )

                // So it cannot claim on sight, and when it does claim it claims above the boundary.
                awaitLeadership(node, expected = true)
                assertEquals(10L, node.logProc.termFence.highestSeen)
            }
        }
    }

    @Test
    fun `the reader discards a fenced record, still advancing the consume position`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                val leader = replicaLog.appendMessage(
                    ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = 2L)
                )
                val superseded = replicaLog.appendMessage(
                    ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), srcMsgId = 2, termId = 1L)
                )

                awaitReplicaMsg(node, superseded.msgId)

                assertEquals(1L, node.watchers.latestTxId, "the superseded leader's tx was never applied")
                assertEquals(2L, node.logProc.termFence.highestSeen)
                assertTrue(leader.msgId < superseded.msgId)
            }
        }
    }

    @Test
    fun `a node applies the messages already on the log, then claims`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            replicaLog.appendMessage(ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), termId = 1))

            TestNode(sourceLog, replicaLog).use { node ->
                node.watchers.awaitTx(1)

                // A term is already on the log, so the claim-on-sight path is closed and this one waits out an election before claiming above it.
                awaitLeadership(node, expected = true)
            }
        }
    }

    @Test
    fun `a block closes even once the fence has moved past the term that cut it`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val bufferPool = mockBufferPool()

            every { bufferPool.getByteArray(TableCatalog.blockFilePath(0)) } returns block { blockIndex = 0 }.toByteArray()

            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 0, termId = cutter))
            // a claim landing between the boundary and its upload is what moves the fence past `cutter`, and under self-election it lands there routinely
            replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 5))
            val uploaded = replicaLog.appendMessage(
                ReplicaMessage.BlockUploaded(Storage.VERSION, 0, 0, 0, emptyList(), termId = cutter)
            )

            TestNode(sourceLog, replicaLog, readOnly = true, bufferPool = bufferPool).use { node ->
                awaitReplicaMsg(node, uploaded.msgId)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "b0 is closed, so the follower is no longer buffering behind it"
                )
                assertEquals(
                    5L, node.logProc.termFence.highestSeen,
                    "the claim it held back still folded, so every reader agrees on what the log has reached"
                )
            }
        }
    }

    @Test
    fun `a promotion finishes the block it inherits and applies what was held behind it`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            // The leader that cut b0 died before uploading it, so the follower still holds the block and the tx behind it.
            // That tx carries the boundary's own source position: a block cut pauses resolution, so nothing can land between a boundary and its upload.
            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
            replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = cutter)
            )

            TestNode(sourceLog, replicaLog).use { node ->
                awaitLeadership(node, expected = true)

                // Ahead of the assertions below: the held tx applies behind the adopt, so this is what says the adopt has happened.
                node.watchers.awaitTx(1)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "the incoming leader finished the block its predecessor left open"
                )
                assertEquals(
                    cutter + 1, node.logProc.termFence.highestSeen,
                    "the held records folded on the drain, up to this leader's own claim"
                )
            }
        }
    }

    @Test
    fun `a promotion whose held record will not apply stops the database`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            // As above, except the held tx carries table data that will not load — so the drain throws once the adopt has consumed the record.
            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
            replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(
                    1, Instant.now(), true, null, mapOf("public/docs" to byteArrayOf(1, 2, 3)),
                    srcMsgId = 1, termId = cutter
                )
            )

            TestNode(sourceLog, replicaLog).use { node ->
                awaitFailure(node)
            }
        }
    }

    @Test
    fun `a claim is taken above a term the follower is still holding`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            // b0 is never uploaded, so the node holds everything after it — the records wait on the block, while the tail has read them and folded their terms.
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 0, termId = 4))
            val held = replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 9))

            TestNode(sourceLog, replicaLog).use { node ->
                awaitReplicaMsg(node, held.msgId)
                assertEquals(9L, node.logProc.termFence.highestSeen, "a held record has still been read")

                awaitLeadership(node, expected = true)
                awaitFence(node, 10L)

                assertEquals(
                    10L, node.logProc.termFence.highestSeen,
                    "the winning claim is one above the term the block was holding"
                )
                assertNull(node.watchers.exception)
            }
        }
    }

    @Test
    fun `a claim the replica log refuses leaves the node following and indexing`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, logsDriver = { inner ->
                object : LogProcessor.LogsDriver by inner {
                    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
                        throw IOException("the replica log refused the write")
                }
            }).use { node ->
                val seeded = replicaLog.appendMessage(
                    ReplicaMessage.ResolvedTx(
                        1, Instant.now(), true, null, emptyMap(),
                        srcMsgId = 1, termId = 1L
                    )
                )

                awaitReplicaMsg(node, seeded.msgId)

                assertFalse(node.logProc.isLeader)
                assertEquals(1L, node.watchers.latestTxId, "a node that cannot claim still indexes what the log holds")
                assertNull(node.watchers.exception, "and stays queryable, having lost nothing it held")
            }
        }
    }

    @Test
    fun `a second upload of a block the term has adopted is dropped`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
            replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = cutter)
            )

            TestNode(sourceLog, replicaLog, logsDriver = { inner ->
                object : LogProcessor.LogsDriver by inner {
                    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata {
                        // The term this node is superseding cut the same boundary, so it produced this block too — and its upload lands first, having started first.
                        // Ahead of ours in the log is what makes it the one every node adopts on, ours included.
                        if (msg is ReplicaMessage.BlockUploaded) inner.appendToReplica(msg.copy(termId = cutter))

                        return inner.appendToReplica(msg)
                    }
                }
            }).use { node ->
                awaitLeadership(node, expected = true)

                // The held tx applies behind the adopt, and the drain that applies it is what carries the term past its own duplicate upload.
                node.watchers.awaitTx(1)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "the first upload closed the block"
                )
                assertTrue(node.logProc.isLeader, "and the second cost the term nothing")
                assertNull(node.watchers.exception)
            }
        }
    }
}
