package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.api.TransactionKey
import xtdb.api.log.*
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.storage.MemoryStorage
import xtdb.trie.TrieCatalog
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.util.debug
import xtdb.util.logger
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds

private val LOG = LogProcessorSimTest::class.logger

@Tag("property")
class LogProcessorSimTest : SimulationTestBase() {

    private lateinit var allocator: RootAllocator
    private lateinit var srcLog: SimLog<SourceMessage>
    private lateinit var replicaLog: SimLog<ReplicaMessage>

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        this.srcLog = SimLog("src", dispatcher, rand)
        this.replicaLog = SimLog("replica", dispatcher, rand)
    }

    @AfterEach
    fun tearDown() {
        replicaLog.close()
        srcLog.close()
        allocator.close()
    }

    /**
     * Simulates the Indexer — ~30% random abort rate, no actual SQL indexing.
     */
    private fun simIndexer() = object : Indexer.ForDatabase {
        override fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: xtdb.arrow.VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?, userMetadata: Any?
        ): ReplicaMessage.ResolvedTx =
            ReplicaMessage.ResolvedTx(
                txId = msgId,
                systemTime = systemTime ?: msgTimestamp,
                committed = rand.nextFloat() > 0.3f,
                error = null,
                tableData = emptyMap()
            )

        override fun addTxRow(txKey: TransactionKey, error: Throwable?): ReplicaMessage.ResolvedTx =
            ReplicaMessage.ResolvedTx(
                txId = txKey.txId,
                systemTime = txKey.systemTime,
                committed = error == null,
                error = error,
                tableData = emptyMap()
            )

        override fun close() {}
    }

    /**
     * Per-node state for simulation tests.
     * Each node has its own block catalog, watchers, live index, etc.
     * but shares the source and replica logs with other nodes.
     */
    private inner class SimNode(
        dbName: String, val bp: BufferPool,
        private val fullEvery: Int, private val txsSinceFlush: AtomicInteger,
    ) : LogProcessor.ProcessorFactory, AutoCloseable {
        val blockCatalog = BlockCatalog(dbName, null)

        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
            every { isFull() } answers {
                if (txsSinceFlush.incrementAndGet() % fullEvery == 0) {
                    txsSinceFlush.set(0)
                    true
                } else false
            }
        }

        val dbState = DatabaseState(
            dbName, blockCatalog,
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )

        val watchers = Watchers(-1)
        private val indexer = simIndexer()
        val dbStorage = DatabaseStorage(srcLog, replicaLog, bp, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)

        override fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.LeaderSystem {
            val proc = LeaderLogProcessor(
                allocator, dbStorage, replicaProducer,
                dbState, indexer, watchers,
                emptySet(), null, blockUploader,
                afterSourceMsgId, afterReplicaMsgId
            )
            return object : LogProcessor.LeaderSystem {
                override val proc get() = proc
                override fun close() = proc.close()
            }
        }

        override fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.TransitionProcessor =
            TransitionLogProcessor(
                allocator, bp, dbState, liveIndex,
                blockUploader,
                replicaProducer, watchers, null,
                afterSourceMsgId, afterReplicaMsgId
            )

        override fun openFollower(
            pendingBlock: PendingBlock?,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.FollowerProcessor =
            FollowerLogProcessor(
                allocator, bp, dbState,
                mockk<Compactor.ForDatabase>(relaxed = true),
                watchers, null, pendingBlock,
                afterSourceMsgId, afterReplicaMsgId
            )

        fun openLogProcessor(scope: CoroutineScope) =
            LogProcessor(this, dbStorage, dbState, watchers, blockUploader, scope)

        override fun close() {
            indexer.close()
        }
    }

    private fun emptyTx(): SourceMessage.Tx =
        SourceMessage.Tx(
            txOps = emptyList<TxOp>().toArrowBytes(allocator),
            systemTime = null,
            defaultTz = ZoneId.of("UTC"),
            user = null,
            userMetadata = null
        )

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks with rebalances`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val bp = MemoryStorage(allocator, epoch = 0)
            val fullEvery = rand.nextInt(2, 5)
            val txsSinceFlush = AtomicInteger(0)
            val node = SimNode("test-db", bp, fullEvery, txsSinceFlush)

            val logProcScope = CoroutineScope(dispatcher + Job())
            node.openLogProcessor(logProcScope).use { logProc ->
                val groupJob = launch(dispatcher) { srcLog.openGroupSubscription(logProc) }

                launch(dispatcher) {
                    val totalActions = rand.nextInt(5, 20)
                    LOG.debug("test: will perform $totalActions actions")
                    repeat(totalActions) { _ ->
                        yield()

                        when (rand.nextInt(3)) {
                            0 -> srcLog.appendMessage(emptyTx())
                            1 -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
                            2 -> srcLog.rebalanceTrigger.send(Unit)
                        }
                    }
                    val lastSrcMsgId = srcLog.latestSubmittedMsgId
                    if (lastSrcMsgId >= 0) node.watchers.awaitSource(lastSrcMsgId)
                }.join()

                groupJob.cancel()
                logProcScope.cancel()

                val sourceTxIds = srcLog.topic
                    .filter { it.message is SourceMessage.Tx || it.message is SourceMessage.LegacyTx }
                    .map { it.msgId }

                val replicaTxIds = replicaLog.topic
                    .map { it.message }
                    .filterIsInstance<ReplicaMessage.ResolvedTx>()
                    .map { it.txId }

                assertEquals(sourceTxIds, replicaTxIds, "every source tx should appear on the replica, in order")

                assertEquals(replicaTxIds, replicaTxIds.sorted(), "replica txIds should be monotonically increasing")

                assertEquals(replicaTxIds.size, replicaTxIds.toSet().size, "replica should have no duplicate txIds")

                val replicaMessages = replicaLog.topic.map { it.message }
                val boundaries = replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>().map { it.blockIndex }
                val uploads = replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>().map { it.blockIndex }
                assertEquals(boundaries, uploads, "every BlockBoundary should have a matching BlockUploaded")

                assertEquals(boundaries.indices.map { it.toLong() }, boundaries,
                    "block indices should be contiguous starting from 0")
            }

            node.close()
            bp.close()
        }

    @RepeatableSimulationTest
    fun `multi-node leadership changes preserve block catalog consistency`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val bp = MemoryStorage(allocator, epoch = 0)
            val fullEvery = rand.nextInt(2, 5)
            val txsSinceFlush = AtomicInteger(0)
            val nodeA = SimNode("test-db", bp, fullEvery, txsSinceFlush)
            val nodeB = SimNode("test-db", bp, fullEvery, txsSinceFlush)

            val scopeA = CoroutineScope(dispatcher + Job())
            val scopeB = CoroutineScope(dispatcher + Job())

            nodeA.openLogProcessor(scopeA).use { logProcA ->
                nodeB.openLogProcessor(scopeB).use { logProcB ->
                    val groupJobA = launch(dispatcher) { srcLog.openGroupSubscription(logProcA) }
                    val groupJobB = launch(dispatcher) { srcLog.openGroupSubscription(logProcB) }

                    launch(dispatcher) {
                        val totalActions = rand.nextInt(5, 20)
                        LOG.debug("test: multi-node will perform $totalActions actions")
                        repeat(totalActions) { _ ->
                            yield()
                            when (rand.nextInt(3)) {
                                0 -> srcLog.appendMessage(emptyTx())
                                1 -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                2 -> srcLog.rebalanceTrigger.send(Unit)
                            }
                        }

                        // final tx so both nodes can reach a consistent source watermark
                        srcLog.appendMessage(emptyTx())
                        val lastSrcMsgId = srcLog.latestSubmittedMsgId
                        if (lastSrcMsgId >= 0) {
                            nodeA.watchers.awaitSource(lastSrcMsgId)
                            nodeB.watchers.awaitSource(lastSrcMsgId)
                        }

                        // ensure followers have processed all replica messages including block events
                        replicaLog.awaitAllDelivered()
                    }.join()

                    groupJobA.cancel()
                    groupJobB.cancel()
                    scopeA.cancel()
                    scopeB.cancel()

                    val sourceTxIds = srcLog.topic
                        .filter { it.message is SourceMessage.Tx || it.message is SourceMessage.LegacyTx }
                        .map { it.msgId }

                    val replicaTxIds = replicaLog.topic
                        .map { it.message }
                        .filterIsInstance<ReplicaMessage.ResolvedTx>()
                        .map { it.txId }

                    assertEquals(sourceTxIds, replicaTxIds, "every source tx should appear on the replica, in order")
                    assertEquals(replicaTxIds, replicaTxIds.sorted(), "replica txIds should be monotonically increasing")
                    assertEquals(replicaTxIds.size, replicaTxIds.toSet().size, "replica should have no duplicate txIds")

                    val replicaMessages = replicaLog.topic.map { it.message }
                    val boundaries = replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>().map { it.blockIndex }
                    val uploads = replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>().map { it.blockIndex }
                    assertEquals(boundaries, uploads, "every BlockBoundary should have a matching BlockUploaded")

                    assertEquals(boundaries.indices.map { it.toLong() }, boundaries,
                        "block indices should be contiguous starting from 0")

                    // block catalog consistency — would catch the stale check bug (#5428)
                    val expectedBlockIndex = replicaMessages
                        .filterIsInstance<ReplicaMessage.BlockUploaded>()
                        .maxOfOrNull { it.blockIndex }

                    assertEquals(expectedBlockIndex, nodeA.blockCatalog.currentBlockIndex,
                        "node A block catalog should match latest uploaded block")
                    assertEquals(expectedBlockIndex, nodeB.blockCatalog.currentBlockIndex,
                        "node B block catalog should match latest uploaded block")

                    // follower convergence — both nodes should agree on latest source watermark
                    assertEquals(nodeA.watchers.latestSourceMsgId, nodeB.watchers.latestSourceMsgId,
                        "both nodes should converge on the same source watermark")
                }
            }

            nodeA.close()
            nodeB.close()
            bp.close()
        }
}
