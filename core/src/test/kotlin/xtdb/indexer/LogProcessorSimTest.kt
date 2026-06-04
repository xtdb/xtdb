package xtdb.indexer

import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestTemplate
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.WithSeed
import xtdb.api.IndexerConfig
import xtdb.api.log.*
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.error.Incorrect
import xtdb.indexer.SimLog.Companion.launchSimLog
import xtdb.indexer.TxIndexer.TxResult
import xtdb.storage.MemoryStorage
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.util.asIid
import xtdb.util.debug
import xtdb.util.logger
import java.nio.ByteBuffer
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

private val LOG = LogProcessorSimTest::class.logger

@Tag("property")
class LogProcessorSimTest : SimulationTestBase() {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator
    private lateinit var srcLog: SimLog<SourceMessage>
    private lateinit var replicaLog: SimLog<ReplicaMessage>

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
        srcLog = SimLog("src", rand)
        replicaLog = SimLog("replica", rand)
    }

    @AfterEach
    fun tearDown() {
        replicaLog.close()
        srcLog.close()
        allocator.close()
        nodeBase.close()
    }

    private inline fun <R> CoroutineScope.withLaunchedSimLogs(block: () -> R): R {
        val job = launch(dispatcher) {
            launchSimLog(srcLog)
            launchSimLog(replicaLog)
        }

        return block().also { job.cancel() }
    }

    private val docsTable = TableRef("test-db", "public", "docs")

    private sealed interface SimAction {
        data class Commit(val rows: List<UUID>) : SimAction
        data object Abort : SimAction
    }

    private fun buildActions(rand: kotlin.random.Random, count: Int): List<SimAction> =
        List(count) {
            if (rand.nextFloat() > 0.1f) {
                val rowCount = rand.nextInt(1, 6)
                SimAction.Commit(List(rowCount) { UUID(rand.nextLong(), rand.nextLong()) })
            } else SimAction.Abort
        }

    /**
     * Test-side `ExternalSource`. Holds a pre-built sequence of `SimAction`s; its
     * `onPartitionAssigned` drains the iterator through whichever node is currently leader,
     * calling `txIndexer.indexTx { … }` from inside the leader's coroutine scope.
     *
     * A single instance is shared across all `SimNode`s in a test. Leadership transitions
     * surface as fresh `onPartitionAssigned` invocations on the same instance — the iterator
     * is shared, so the next leader resumes draining from where the previous left off.
     * `close()` is a no-op so the per-`LeaderLogProcessor` `extSource.close()` doesn't tear
     * down the shared instance.
     *
     * Putting `indexTx` inside `onPartitionAssigned` (rather than calling it from the test
     * driver against a stale `TxIndexer` reference) is what keeps the leader's allocator
     * accounting clean across rebalances: `indexTx` allocates an `OpenTx` from the
     * leader's allocator; if `LeaderLogProcessor.close()` runs while that `OpenTx` is still
     * live, `allocator.close()` throws on the outstanding allocation. Holding the call
     * inside `onPartitionAssigned` ties its lifetime to the leader's scope — `extJob.cancel()`
     * propagates cancellation through `indexTx`'s inner catch, which closes the `OpenTx`
     * before the allocator does.
     */
    private inner class SimExtSource(private val actions: List<SimAction>) : ExternalSource {
        private val watchersList = mutableListOf<Watchers>()

        fun watch(watchers: Watchers) {
            watchersList += watchers
        }

        private val nextActionIdxState = MutableStateFlow(0)

        suspend fun awaitQuiescence() = nextActionIdxState.first { it == actions.size }

        override suspend fun onPartitionAssigned(
            partition: Int,
            afterToken: ExternalSourceToken?,
            txIndexer: TxIndexer,
        ) {
            var actionIdx = afterToken?.let { ByteBuffer.wrap(it).getInt() + 1 } ?: 0
            nextActionIdxState.value = actionIdx
            while (actionIdx < actions.size) {
                yield()
                val action = actions[actionIdx]

                val externalSourceToken = ByteArray(Integer.BYTES).also { ByteBuffer.wrap(it).putInt(actionIdx) }
                txIndexer.indexTx(externalSourceToken = externalSourceToken) { openTx ->
                    when (action) {
                        is SimAction.Commit -> {
                            val table = openTx.table(docsTable)
                            for (id in action.rows) {
                                table.logPut(
                                    ByteBuffer.wrap(id.asIid),
                                    openTx.systemFrom,
                                    Long.MAX_VALUE,
                                ) {
                                    table.docWriter.writeObject(
                                        mapOf("_id" to id, "tx_id" to openTx.txKey.txId),
                                    )
                                }
                            }
                            TxResult.Committed()
                        }

                        SimAction.Abort -> TxResult.Aborted(Incorrect("aborted"))
                    }
                }

                nextActionIdxState.value = ++actionIdx
            }
        }

        override fun close() {}
    }

    private inner class SimNode(
        dbName: String,
        val bp: MemoryStorage,
        indexerConfig: IndexerConfig,
        private val simExtSource: SimExtSource,
    ) : LogProcessor.ProcessorFactory, AutoCloseable {

        val blockCatalog = BlockCatalog(dbName, null)
        val tableCatalog = TableCatalog(bp)
        val trieCatalog = createTrieCatalog()
        val liveIndex = LiveIndex.open(allocator, blockCatalog, tableCatalog, trieCatalog, dbName, indexerConfig)

        val dbState = DatabaseState(dbName, blockCatalog, tableCatalog, trieCatalog, liveIndex)

        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
            .also { simExtSource.watch(it) }
        val dbStorage = DatabaseStorage(srcLog, replicaLog, bp, null)
        val blockUploader =
            BlockUploader(dbStorage, dbState, mockk(relaxed = true), null, null, uploadDispatcher = dispatcher)
        val crashLogger = CrashLogger(allocator, bp, "sim-node")

        override fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.LeaderSystem {
            val proc = LeaderLogProcessor(
                allocator, nodeBase, dbStorage, crashLogger,
                dbState, blockUploader, watchers,
                extSource = simExtSource, replicaProducer = replicaProducer,
                skipTxs = emptySet(), dbCatalog = null,
                partition = 0, afterReplicaMsgId = afterReplicaMsgId,
                ctx = dispatcher
            )
            return object : LogProcessor.LeaderSystem {
                override val proc get() = proc
                override suspend fun cancelAndJoin() = proc.cancelAndJoin()
            }
        }

        override fun openTransition(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.TransitionProcessor =
            TransitionLogProcessor(
                allocator, bp, dbState, liveIndex,
                blockUploader,
                replicaProducer, watchers, null,
                afterReplicaMsgId,
                hasExternalSource = true,
            )

        override fun openFollower(
            pendingBlock: PendingBlock?,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.FollowerProcessor =
            FollowerLogProcessor(
                allocator, bp, dbState,
                mockk<Compactor.ForDatabase>(relaxed = true),
                watchers, null, pendingBlock,
                afterReplicaMsgId,
                hasExternalSource = true,
            )

        fun openLogProcessor(scope: CoroutineScope) =
            LogProcessor(this, dbStorage, dbState, watchers, blockUploader, scope)

        override fun close() {
            dbState.close()
        }
    }

    private fun assertBlockFilesExist(bp: MemoryStorage, dbName: String, replicaMessages: List<ReplicaMessage>) {
        val storedPaths = bp.listAllObjects().map { it.key }.toSet()

        for (upload in replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()) {
            val blockIdx = upload.blockIndex

            assertTrue(
                BlockCatalog.blockFilePath(blockIdx) in storedPaths,
                "block file missing for b$blockIdx"
            )

            val tables = upload.tries.map { TableRef.parse(dbName, it.tableName) }.toSet()

            for (trie in upload.tries) {
                val table = TableRef.parse(dbName, trie.tableName)
                assertTrue(
                    table.dataFilePath(trie.trieKey) in storedPaths,
                    "data file missing for ${trie.tableName}/${trie.trieKey}"
                )
                assertTrue(
                    table.metaFilePath(trie.trieKey) in storedPaths,
                    "meta file missing for ${trie.tableName}/${trie.trieKey}"
                )
            }

            for (table in tables) {
                assertTrue(
                    BlockCatalog.tableBlockPath(table, blockIdx) in storedPaths,
                    "table-block file missing for ${table.schemaAndTable}/b$blockIdx"
                )
            }
        }
    }

    private fun abortedTxIds(): Set<MessageId> =
        replicaLog.topic.map { it.message }
            .filterIsInstance<ReplicaMessage.ResolvedTx>()
            .filter { !it.committed }
            .map { it.txId }
            .toSet()

    private fun assertSnapshotHasNoAbortedRows(node: SimNode) {
        node.liveIndex.openSnapshot().use { snap ->
            assertEquals(
                node.liveIndex.latestCompletedTx?.txId, snap.txBasis?.txId,
                "snapshot basis should equal liveIndex.latestCompletedTx"
            )

            val basisTxId = snap.txBasis?.txId ?: -1L

            for (tableSnap in snap.table(docsTable)) {
                val rel = tableSnap.relation
                val op = rel["op"]
                val put = op.vectorForOrNull("put") ?: continue
                val txIdVec = put.vectorFor("tx_id")

                for (i in 0 until rel.rowCount) {
                    if (op.getLeg(i) == "put") {
                        val txId = txIdVec.getLong(i)
                        assertTrue(
                            txId !in abortedTxIds(),
                            "aborted txId=$txId left a row in live table"
                        )
                        assertTrue(
                            txId <= basisTxId,
                            "row txId=$txId > snapshot basis=$basisTxId"
                        )
                    }
                }
            }
        }
    }

    private fun replicaTxIds(): List<MessageId> =
        replicaLog.topic.map { it.message }
            .filterIsInstance<ReplicaMessage.ResolvedTx>()
            .map { it.txId }

    private fun assertReplicaTxInvariants() {
        val replicaTxIds = replicaTxIds()
        assertEquals(
            replicaTxIds, replicaTxIds.sorted(),
            "replica txIds should be monotonically increasing"
        )
        assertEquals(
            replicaTxIds.size, replicaTxIds.toSet().size,
            "replica should have no duplicate txIds"
        )
    }

    private fun assertBlockBoundariesMatchUploads(replicaMessages: List<ReplicaMessage>) {
        val boundaries = replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>().map { it.blockIndex }
        val uploads = replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>().map { it.blockIndex }
        assertEquals(boundaries, uploads, "every BlockBoundary should have a matching BlockUploaded")
        assertEquals(
            boundaries.indices.map { it.toLong() }, boundaries,
            "block indices should be contiguous starting from 0"
        )
    }

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks with rebalances`() =
        runTest(timeout = 5.seconds) {
            withLaunchedSimLogs {
                val rowsPerBlock = rand.nextLong(15, 25)
                val totalActions = rand.nextInt(50, 100)
                val actions = buildActions(rand, totalActions)
                val simExtSource = SimExtSource(actions)
                val srcLogEventCount = rand.nextInt(20, 40)
                LOG.debug("test: $totalActions actions, $srcLogEventCount srcLogEvents (rowsPerBlock=$rowsPerBlock)")

                MemoryStorage(allocator, epoch = 0).use { bp ->
                    SimNode("test-db", bp, IndexerConfig(rowsPerBlock = rowsPerBlock), simExtSource).use { node ->
                        val logProcScope = CoroutineScope(dispatcher + Job(coroutineContext.job))
                        val logProc = node.openLogProcessor(logProcScope)
                        try {
                            val groupJob = launch(dispatcher) { srcLog.openGroupSubscription(logProc) }

                            launch(dispatcher) {
                                repeat(srcLogEventCount) {
                                    yield()
                                    if (rand.nextInt(100) < 50) {
                                        srcLog.rebalanceTrigger.send(Unit)
                                    } else {
                                        srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                    }
                                }
                                simExtSource.awaitQuiescence()

                                replicaLog.awaitAllDelivered()
                            }.join()

                            groupJob.cancel()
                            logProcScope.cancel()

                            assertReplicaTxInvariants()
                            assertEquals(
                                totalActions, replicaTxIds().size,
                                "all actions should appear on the replica"
                            )

                            val replicaMessages = replicaLog.topic.map { it.message }
                            assertBlockBoundariesMatchUploads(replicaMessages)

                            val expectedBlockIndex =
                                replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()
                                    .maxOfOrNull { it.blockIndex }
                            assertEquals(
                                expectedBlockIndex, node.blockCatalog.currentBlockIndex,
                                "block catalog should match latest uploaded block"
                            )

                            val replicaTxIds = replicaTxIds()
                            if (replicaTxIds.isNotEmpty()) {
                                val lastReplicaTx = replicaMessages
                                    .filterIsInstance<ReplicaMessage.ResolvedTx>().last()
                                assertEquals(
                                    lastReplicaTx.txId, node.liveIndex.latestCompletedTx?.txId,
                                    "live index latestCompletedTx should match last replica tx"
                                )
                            }

                            assertBlockFilesExist(bp, "test-db", replicaMessages)
                            assertSnapshotHasNoAbortedRows(node)
                        } finally {
                            logProc.cancelAndJoin()
                        }
                    }
                }
            }
        }

    @RepeatableSimulationTest
    fun `stable leader with sustained throughput`() = runTest(timeout = 5.seconds) {
        withLaunchedSimLogs {
            val rowsPerBlock = rand.nextLong(15, 25)
            val indexerConfig = IndexerConfig(rowsPerBlock = rowsPerBlock)
            val totalActions = rand.nextInt(50, 100)
            val actions = buildActions(rand, totalActions)
            val simExtSource = SimExtSource(actions)
            val srcLogEventCount = rand.nextInt(5, 15)
            LOG.debug("test: stable-leader $totalActions actions, $srcLogEventCount FlushBlocks (rowsPerBlock=$rowsPerBlock)")

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, indexerConfig, simExtSource).use { leader ->
                    SimNode("test-db", bp, indexerConfig, simExtSource).use { followerA ->
                        SimNode("test-db", bp, indexerConfig, simExtSource).use { followerB ->
                            val leaderScope = CoroutineScope(dispatcher + Job(coroutineContext.job))
                            val followerScopeA = CoroutineScope(dispatcher + Job(coroutineContext.job))
                            val followerScopeB = CoroutineScope(dispatcher + Job(coroutineContext.job))

                            val leaderProc = leader.openLogProcessor(leaderScope)
                            val followerProcA = followerA.openLogProcessor(followerScopeA)
                            val followerProcB = followerB.openLogProcessor(followerScopeB)
                            try {
                                val groupJobLeader =
                                    launch(dispatcher) { srcLog.openGroupSubscription(leaderProc) }
                                val groupJobA =
                                    launch(dispatcher) { srcLog.openGroupSubscription(followerProcA) }
                                val groupJobB =
                                    launch(dispatcher) { srcLog.openGroupSubscription(followerProcB) }

                                launch(dispatcher) {
                                    repeat(srcLogEventCount) {
                                        yield()
                                        srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                    }
                                    simExtSource.awaitQuiescence()
                                    replicaLog.awaitAllDelivered()

                                    // Anchor the per-node `latestTxId` to the latest replica tx so the
                                    // convergence assertions below see consistent state across nodes.
                                    val lastReplicaTxId = replicaLog.topic.map { it.message }
                                        .filterIsInstance<ReplicaMessage.ResolvedTx>()
                                        .maxOfOrNull { it.txId }
                                    if (lastReplicaTxId != null) {
                                        leader.watchers.awaitTx(lastReplicaTxId)
                                        followerA.watchers.awaitTx(lastReplicaTxId)
                                        followerB.watchers.awaitTx(lastReplicaTxId)
                                    }
                                }.join()

                                groupJobLeader.cancel()
                                groupJobA.cancel()
                                groupJobB.cancel()
                                leaderScope.cancel()
                                followerScopeA.cancel()
                                followerScopeB.cancel()

                                assertReplicaTxInvariants()
                                assertEquals(
                                    totalActions, replicaTxIds().size,
                                    "all actions should appear on the replica"
                                )

                                val replicaMessages = replicaLog.topic.map { it.message }
                                assertBlockBoundariesMatchUploads(replicaMessages)

                                val nodes = listOf(leader, followerA, followerB)

                                val expectedBlockIndex =
                                    replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()
                                        .maxOfOrNull { it.blockIndex }
                                for (node in nodes) {
                                    assertEquals(
                                        expectedBlockIndex, node.blockCatalog.currentBlockIndex,
                                        "block catalog should match latest uploaded block"
                                    )
                                }

                                val expectedLatestCompletedTx = leader.liveIndex.latestCompletedTx
                                val expectedBlockCatalogTx = leader.blockCatalog.latestCompletedTx
                                val expectedProcessedMsgId = leader.blockCatalog.latestProcessedMsgId

                                for (node in nodes) {
                                    assertEquals(
                                        expectedProcessedMsgId, node.blockCatalog.latestProcessedMsgId,
                                        "all nodes should agree on latestProcessedMsgId"
                                    )
                                    assertEquals(
                                        expectedBlockCatalogTx, node.blockCatalog.latestCompletedTx,
                                        "all nodes should agree on block catalog's latestCompletedTx"
                                    )
                                    assertEquals(
                                        expectedLatestCompletedTx, node.liveIndex.latestCompletedTx,
                                        "all nodes should agree on live index's latestCompletedTx"
                                    )
                                }

                                assertBlockFilesExist(bp, "test-db", replicaMessages)

                                assertSnapshotHasNoAbortedRows(leader)
                                assertSnapshotHasNoAbortedRows(followerA)
                                assertSnapshotHasNoAbortedRows(followerB)
                            } finally {
                                leaderProc.cancelAndJoin()
                                followerProcA.cancelAndJoin()
                                followerProcB.cancelAndJoin()
                            }
                        }
                    }
                }
            }
        }
    }

    @RepeatableSimulationTest
    fun `multi-node leadership changes preserve block catalog consistency`() =
        runTest(timeout = 5.seconds) {
            withLaunchedSimLogs {
                val rowsPerBlock = rand.nextLong(15, 25)
                val indexerConfig = IndexerConfig(rowsPerBlock = rowsPerBlock)
                val totalActions = rand.nextInt(50, 100)
                val actions = buildActions(rand, totalActions)
                val simExtSource = SimExtSource(actions)
                val srcLogEventCount = rand.nextInt(20, 40)
                LOG.debug("test: multi-node $totalActions actions, $srcLogEventCount srcLogEvents (rowsPerBlock=$rowsPerBlock)")

                MemoryStorage(allocator, epoch = 0).use { bp ->
                    SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeA ->
                        SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeB ->
                            val scopeA = CoroutineScope(dispatcher + Job(coroutineContext.job))
                            val scopeB = CoroutineScope(dispatcher + Job(coroutineContext.job))

                            val logProcA = nodeA.openLogProcessor(scopeA)
                            val logProcB = nodeB.openLogProcessor(scopeB)
                            try {
                                val groupJobA = launch(dispatcher) { srcLog.openGroupSubscription(logProcA) }
                                val groupJobB = launch(dispatcher) { srcLog.openGroupSubscription(logProcB) }

                                launch(dispatcher) {
                                    repeat(srcLogEventCount) {
                                        yield()
                                        if (rand.nextInt(100) < 50) {
                                            srcLog.rebalanceTrigger.send(Unit)
                                        } else {
                                            srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                        }
                                    }
                                    simExtSource.awaitQuiescence()
                                    replicaLog.awaitAllDelivered()

                                    // Anchor the per-node `latestTxId` to the latest replica tx so the
                                    // convergence assertions below see consistent state across nodes.
                                    val lastReplicaTxId = replicaLog.topic.map { it.message }
                                        .filterIsInstance<ReplicaMessage.ResolvedTx>()
                                        .maxOfOrNull { it.txId }
                                    if (lastReplicaTxId != null) {
                                        nodeA.watchers.awaitTx(lastReplicaTxId)
                                        nodeB.watchers.awaitTx(lastReplicaTxId)
                                    }
                                }.join()

                                groupJobA.cancel()
                                groupJobB.cancel()
                                scopeA.cancel()
                                scopeB.cancel()

                                assertReplicaTxInvariants()
                                assertEquals(
                                    totalActions, replicaTxIds().size,
                                    "all actions should appear on the replica"
                                )

                                val replicaMessages = replicaLog.topic.map { it.message }
                                assertBlockBoundariesMatchUploads(replicaMessages)

                                val expectedBlockIndex = replicaMessages
                                    .filterIsInstance<ReplicaMessage.BlockUploaded>()
                                    .maxOfOrNull { it.blockIndex }

                                assertEquals(
                                    expectedBlockIndex, nodeA.blockCatalog.currentBlockIndex,
                                    "node A block catalog should match latest uploaded block"
                                )
                                assertEquals(
                                    expectedBlockIndex, nodeB.blockCatalog.currentBlockIndex,
                                    "node B block catalog should match latest uploaded block"
                                )

                                assertEquals(
                                    nodeA.blockCatalog.latestProcessedMsgId, nodeB.blockCatalog.latestProcessedMsgId,
                                    "both nodes should agree on latestProcessedMsgId"
                                )

                                assertEquals(
                                    nodeA.blockCatalog.latestCompletedTx, nodeB.blockCatalog.latestCompletedTx,
                                    "both nodes should agree on block catalog's latestCompletedTx"
                                )

                                assertEquals(
                                    nodeA.liveIndex.latestCompletedTx, nodeB.liveIndex.latestCompletedTx,
                                    "both nodes should agree on live index's latestCompletedTx"
                                )

                                assertBlockFilesExist(bp, "test-db", replicaMessages)

                                assertSnapshotHasNoAbortedRows(nodeA)
                                assertSnapshotHasNoAbortedRows(nodeB)
                            } finally {
                                logProcA.cancelAndJoin()
                                logProcB.cancelAndJoin()
                            }
                        }
                    }
                }
            }
        }
}
