package xtdb.indexer

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
import xtdb.TestPartition
import xtdb.api.IndexerConfig
import xtdb.api.log.*
import xtdb.block.proto.block
import xtdb.api.storage.Storage
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.storage.MemoryStorage
import xtdb.types.MessageId
import java.io.IOException
import java.nio.ByteBuffer
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

    /** One node's worth of the fixture, torn down as a unit. */
    private inner class TestNode(
        sourceLog: InMemoryLog<SourceMessage>,
        replicaLog: InMemoryLog<ReplicaMessage>,
        boundaryTermId: Long? = null,
        boundaryTermSeq: Long? = null,
        readOnly: Boolean = false,
        // A quarter of the in-process scale, so a case turning on an empty poll settles within awaitLeadership's budget.
        // The 5-10x election range comes off this, as in production.
        electionDriver: ElectionDriver = RealElectionDriver(assertInterval = 25.milliseconds),
        val bufferPool: MemoryStorage = MemoryStorage(allocator, epoch = 0),
        logsDriver: (LogProcessor.LogsDriver) -> LogProcessor.LogsDriver = { it },
    ) : AutoCloseable {
        private val partition = TestPartition(
            allocator, bufferPool, sourceLog, replicaLog,
            block = boundaryTermId?.let {
                block {
                    blockIndex = 0
                    termId = it
                    boundaryTermSeq?.let { seq -> this.boundaryTermSeq = seq }
                }
            },
        )

        val partitionState get() = partition.state
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val scope = CoroutineScope(SupervisorJob())

        val logProc = LogProcessor(
            allocator, nodeBase, mockk(relaxed = true),
            partition.storage, partition.state, "test-db", watchers,
            mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
            externalSource = null,
            scope = scope,
            flushTimeout = IndexerConfig().flushDuration,
            logsDriver = logsDriver(LogProcessor.RealLogsDriver(partition.storage)),
            electionDriver = electionDriver,
            readOnly = readOnly,
        )

        /** Write the block file a [ReplicaMessage.BlockUploaded] for [blockIndex] sends a reader to read. */
        fun writeBlockFile(blockIndex: Long) =
            bufferPool.putObjectSync(
                TableCatalog.blockFilePath(blockIndex),
                ByteBuffer.wrap(block { this.blockIndex = blockIndex }.toByteArray())
            )

        override fun close() {
            runBlocking { scope.coroutineContext.job.cancelAndJoin() }
            logProc.close()
            partition.close()
            bufferPool.close()
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
            withTimeout(5_000) { while (node.logProc.highestTermSeen < term) yield() }
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
                    1L, node.logProc.highestTermSeen,
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
                assertEquals(2L, node.logProc.highestTermSeen)
            }
        }
    }

    @Test
    fun `the fence seeds from the persisted block boundary and only rises`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, boundaryTermId = 9L).use { node ->
                assertEquals(
                    9L, node.logProc.highestTermSeen,
                    "a node that has flushed a block starts from the term that cut it"
                )

                // So it cannot claim on sight, and when it does claim it claims above the boundary.
                awaitLeadership(node, expected = true)
                assertEquals(10L, node.logProc.highestTermSeen)
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
                assertEquals(2L, node.logProc.highestTermSeen)
                assertTrue(leader.msgId < superseded.msgId)
            }
        }
    }

    private fun tx(txId: Long, termId: Long, termSeq: Long?) =
        ReplicaMessage.ResolvedTx(
            txId, Instant.now(), true, null, emptyMap(), srcMsgId = txId, termId = termId, termSeq = termSeq
        )

    @Test
    fun `a record missing from a term voids the rest of that term, and the next term applies again`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1, termSeq = 0))
                replicaLog.appendMessage(tx(1, termId = 1, termSeq = 1))
                replicaLog.appendMessage(tx(3, termId = 1, termSeq = 3))
                val voided = replicaLog.appendMessage(tx(4, termId = 1, termSeq = 4))

                awaitReplicaMsg(node, voided.msgId)
                assertEquals(1L, node.watchers.latestTxId, "nothing after the gap applied")

                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2, termSeq = 0))
                val next = replicaLog.appendMessage(tx(5, termId = 2, termSeq = 1))

                awaitReplicaMsg(node, next.msgId)
                assertEquals(5L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a losing claim among the winning leader's records neither applies nor voids the term`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1, termSeq = 0))
                replicaLog.appendMessage(tx(1, termId = 1, termSeq = 1))
                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1, termSeq = 0))
                // A losing claim from a node predating term positions.
                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1))
                val last = replicaLog.appendMessage(tx(2, termId = 1, termSeq = 2))

                awaitReplicaMsg(node, last.msgId)
                assertEquals(2L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a reader resuming after a block checks the term from the position after its boundary`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, boundaryTermId = 3, boundaryTermSeq = 5, readOnly = true).use { node ->
                replicaLog.appendMessage(tx(1, termId = 3, termSeq = 6))
                val voided = replicaLog.appendMessage(tx(2, termId = 3, termSeq = 8))

                awaitReplicaMsg(node, voided.msgId)
                assertEquals(1L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a new term whose claim is missing is voided from its first record`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                replicaLog.appendMessage(tx(1, termId = 1, termSeq = 1))
                val last = replicaLog.appendMessage(tx(2, termId = 1, termSeq = 2))

                awaitReplicaMsg(node, last.msgId)
                assertEquals(-1L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a follower that reads a gap claims the next term without waiting out an election`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1, termSeq = 0))
            replicaLog.appendMessage(tx(1, termId = 1, termSeq = 1))

            TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
                awaitFence(node, 1)

                replicaLog.appendMessage(tx(3, termId = 1, termSeq = 3))

                awaitLeadership(node, expected = true)
                assertEquals(2L, node.logProc.highestTermSeen)
            }
        }
    }

    @Test
    fun `a term won by a claim with no position is not checked`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1))
                replicaLog.appendMessage(tx(1, termId = 1, termSeq = null))
                val last = replicaLog.appendMessage(tx(2, termId = 1, termSeq = 7))

                awaitReplicaMsg(node, last.msgId)
                assertEquals(2L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a reader resuming after a boundary with no position takes the next record's as given`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, boundaryTermId = 3, readOnly = true).use { node ->
                replicaLog.appendMessage(tx(1, termId = 3, termSeq = 9))
                replicaLog.appendMessage(tx(2, termId = 3, termSeq = 10))
                val voided = replicaLog.appendMessage(tx(3, termId = 3, termSeq = 12))

                awaitReplicaMsg(node, voided.msgId)
                assertEquals(2L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a losing claim read after a boundary with no position leaves the term's mode to its leader's records`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, boundaryTermId = 3, readOnly = true).use { node ->
                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 3, termSeq = 0))
                replicaLog.appendMessage(tx(1, termId = 3, termSeq = null))
                val last = replicaLog.appendMessage(tx(2, termId = 3, termSeq = null))

                awaitReplicaMsg(node, last.msgId)
                assertEquals(2L, node.watchers.latestTxId)
            }
        }
    }

    @Test
    fun `a leader that reads back a gap in its own term stands down and claims the next`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            TestNode(sourceLog, replicaLog, electionDriver = noElectionTimeout()).use { node ->
                awaitLeadership(node, expected = true)

                replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 1, termSeq = 5))

                awaitFence(node, 2)
                awaitLeadership(node, expected = true)
                assertEquals(2L, node.logProc.highestTermSeen, "the only node leads again, at the term above the voided one")
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
    fun `a block stays open on an upload from a term the fence has moved past`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val cutter = 4L
            val successor = 5L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 0, termId = cutter))
            // a claim landing between the boundary and its upload is what moves the fence past `cutter`, and under self-election it lands there routinely
            replicaLog.appendMessage(ReplicaMessage.NoOp(termId = successor))
            val staleUpload = replicaLog.appendMessage(
                ReplicaMessage.BlockUploaded(Storage.VERSION, 0, 0, 0, emptyList(), termId = cutter)
            )

            TestNode(sourceLog, replicaLog, readOnly = true).use { node ->
                node.writeBlockFile(0)

                awaitReplicaMsg(node, staleUpload.msgId)

                assertNull(
                    node.partitionState.tableCatalog.currentBlockIndex,
                    "the superseded term's upload is discarded like any other record it wrote"
                )

                val reUpload = replicaLog.appendMessage(
                    ReplicaMessage.BlockUploaded(Storage.VERSION, 0, 0, 0, emptyList(), termId = successor)
                )

                awaitReplicaMsg(node, reUpload.msgId)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "b0 closes on the successor re-uploading it, so the follower stops buffering behind it"
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
                    cutter + 1, node.logProc.highestTermSeen,
                    "every record up to this leader's own claim folded where it arrived, held ones included"
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
                assertEquals(9L, node.logProc.highestTermSeen, "a held record has still been read")

                awaitLeadership(node, expected = true)
                awaitFence(node, 10L)

                assertEquals(
                    10L, node.logProc.highestTermSeen,
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
                    override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> =
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
    fun `a promotion that fails leaves a follower holding the block it produced`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
            val held = replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = cutter)
            )

            // The promotion writes the block's files and then cannot announce it, which is the window between
            // stopping the follower and publishing the leader.
            TestNode(sourceLog, replicaLog, logsDriver = { inner ->
                object : LogProcessor.LogsDriver by inner {
                    override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> =
                        if (msg is ReplicaMessage.BlockUploaded) throw IOException("the replica log refused the upload")
                        else inner.enqueueToReplica(msg)
                }
            }).use { node ->
                awaitFence(node, cutter + 1)
                awaitReplicaMsg(node, held.msgId)

                assertFalse(node.logProc.isLeader, "the promotion failed, so nothing is leading")
                assertNull(node.watchers.exception, "and the database is still readable")

                // Whoever leads next produces the same block, which the re-opened follower is still holding.
                val reUpload = replicaLog.appendMessage(
                    ReplicaMessage.BlockUploaded(
                        Storage.VERSION, 0, 0, 1, emptyList(), termId = cutter + 1, termSeq = 1
                    )
                )

                awaitReplicaMsg(node, reUpload.msgId)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "the follower the failed promotion left behind closed the block"
                )
                assertEquals(1L, node.watchers.latestTxId, "and applied what was held behind it")
            }
        }
    }

    @Test
    fun `a leader superseded mid-block hands that block to the follower replacing it`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
            replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = cutter)
            )

            val superseding = 99L

            // A rival's claim lands ahead of this term's own upload, so the term reads itself superseded
            // while still holding the block it has just produced.
            TestNode(sourceLog, replicaLog, logsDriver = { inner ->
                object : LogProcessor.LogsDriver by inner {
                    override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> {
                        if (msg is ReplicaMessage.BlockUploaded)
                            inner.appendToReplica(ReplicaMessage.NoOp(termId = superseding))

                        return inner.enqueueToReplica(msg)
                    }
                }
            }).use { node ->
                awaitFence(node, superseding)
                awaitLeadership(node, expected = false)

                // The rival produces the same block; the block this node holds has to survive the resignation
                // to close on it, or the upload reaches a role with nothing to match it against.
                val reUpload = replicaLog.appendMessage(
                    ReplicaMessage.BlockUploaded(Storage.VERSION, 0, 0, 1, emptyList(), termId = superseding)
                )

                awaitReplicaMsg(node, reUpload.msgId)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "the follower that replaced the term closed the block that term was holding"
                )
                assertEquals(1L, node.watchers.latestTxId, "and applied what was held behind it")
                assertNull(node.watchers.exception)
            }
        }
    }

    @Test
    fun `an upload from the term this one superseded is dropped`() = runTest {
        withFreshLogs { sourceLog, replicaLog ->
            val cutter = 4L
            replicaLog.appendMessage(ReplicaMessage.BlockBoundary(0, 1, termId = cutter))
            replicaLog.appendMessage(
                ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = cutter)
            )

            TestNode(sourceLog, replicaLog, logsDriver = { inner ->
                object : LogProcessor.LogsDriver by inner {
                    override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> {
                        // The term this node is superseding cut the same boundary, so it produced this block too — and its upload lands ahead of ours, having started first.
                        if (msg is ReplicaMessage.BlockUploaded) inner.appendToReplica(msg.copy(termId = cutter))

                        return inner.enqueueToReplica(msg)
                    }
                }
            }).use { node ->
                awaitLeadership(node, expected = true)

                // The held tx applies behind the adopt, so this is what says the block closed.
                node.watchers.awaitTx(1)

                assertEquals(
                    0L, node.partitionState.tableCatalog.currentBlockIndex,
                    "the block closed on this term's own upload, the superseded term's having been fenced"
                )
                assertTrue(node.logProc.isLeader, "and the fenced one cost the term nothing")
                assertNull(node.watchers.exception)
            }
        }
    }
}
