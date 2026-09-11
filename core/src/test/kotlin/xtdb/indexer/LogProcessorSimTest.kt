package xtdb.indexer

import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
import xtdb.api.Xtdb
import xtdb.api.log.*
import xtdb.api.log.ReplicaMessage.NoOp
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.error.Incorrect
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.storage.MemoryStorage
import xtdb.api.TableRef
import xtdb.table.TableSlug
import xtdb.table.fromSchemaAndTable
import xtdb.util.debug
import xtdb.util.logger
import java.nio.ByteBuffer
import java.util.*
import kotlin.time.Duration.Companion.seconds
import xtdb.api.tx.TxIndexer
import xtdb.types.LogOffset
import xtdb.types.MessageId

private val LOG = LogProcessorSimTest::class.logger

/**
 * One node's handle on the replica log, recording where its own appends landed.
 *
 * The log is a single object every node shares, so a record on it carries no author — and who wrote
 * what is the whole subject of the election properties below.
 */
private class NodeReplicaLog(private val log: Log<ReplicaMessage>) : Log<ReplicaMessage> by log {
    val appendedOffsets = mutableSetOf<LogOffset>()

    override suspend fun appendMessage(message: ReplicaMessage, partition: Int) =
        log.appendMessage(message, partition).also { appendedOffsets += it.logOffset }
}

@Tag("property")
class LogProcessorSimTest : SimulationTestBase() {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator
    private lateinit var srcLog: SimLog<SourceMessage>
    private lateinit var replicaLog: SimLog<ReplicaMessage>

    @BeforeEach
    fun setUp() {
        // [assertBlockFilesExist] requires every uploaded block file to survive; these sims cut well past `blocksToKeep`.
        nodeBase = openBase(Xtdb.Config().garbageCollector { enabled = false }, openMeterRegistry = false)
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

    private val docsTable = TableRef("public", "docs")

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
     * calling `txIndexer.execute { … }` from inside the leader's coroutine scope.
     *
     * A single instance is shared across all `SimNode`s in a test. Leadership transitions
     * surface as fresh `onPartitionAssigned` invocations on the same instance — the iterator
     * is shared, so the next leader resumes draining from where the previous left off.
     *
     * Putting `execute` inside `onPartitionAssigned` (rather than calling it from the test
     * driver against a stale `TxIndexer` reference) is what keeps the leader's allocator
     * accounting clean across rebalances: `execute` allocates an `OpenTx` from the
     * leader's allocator; if the leader term's scope were cancelled while that `OpenTx` is
     * still live, the leader's `allocator.close()` would throw on the
     * outstanding allocation. Holding the call inside `onPartitionAssigned` ties its lifetime
     * to the leader's scope — cancelling that scope propagates cancellation through `execute`'s
     * inner catch, which closes the `OpenTx` before the allocator does.
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
                txIndexer.executeTx(externalSourceToken = externalSourceToken) { openTx ->
                    when (action) {
                        is SimAction.Commit -> {
                            val table = openTx.table(docsTable)
                            for (id in action.rows) {
                                table.writePut(mapOf("_id" to id, "tx_id" to openTx.txKey.txId))
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
        private val dbName: String,
        val bp: MemoryStorage,
        private val indexerConfig: IndexerConfig,
        private val simExtSource: SimExtSource,
        private val readOnly: Boolean = false,
    ) : AutoCloseable {

        val tableCatalog = TableCatalog(bp)
        val trieCatalog = createTrieCatalog()
        val liveIndex =
            LiveIndex.open(allocator, tableCatalog, trieCatalog, indexerConfig, ioDispatcher = dispatcher)

        val partitionState = PartitionState(tableCatalog, trieCatalog, liveIndex)

        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
            .also { simExtSource.watch(it) }
        val nodeReplicaLog = NodeReplicaLog(replicaLog)
        val partitionStorage = PartitionStorage(DatabaseLogs(srcLog, nodeReplicaLog), bp, null)
        val crashLogger = CrashLogger(allocator, bp, "sim-node")

        private var logProcessor: LogProcessor? = null

        val isLeader get() = logProcessor?.isLeader ?: false

        fun openLogProcessor(scope: CoroutineScope) =
            LogProcessor(
                allocator, nodeBase, crashLogger,
                partitionStorage, partitionState, dbName, watchers,
                mockk<Compactor.ForDatabase>(relaxed = true), dbCatalog = null,
                externalSource = simExtSource,
                scope = scope,
                flushTimeout = indexerConfig.flushDuration,
                ioDispatcher = dispatcher,
                // A real `onTimeout` would schedule on kotlinx's own timer thread, which DeterministicDispatcher checks against — a real clock firing into a seeded harness.
                electionDriver = NoAssertElectionDriver,
                readOnly = readOnly,
            ).also { logProcessor = it }

        override fun close() {
            logProcessor?.close()
            partitionState.close()
        }
    }

    private fun assertBlockFilesExist(bp: MemoryStorage, dbName: String, replicaMessages: List<ReplicaMessage>) {
        val storedPaths = bp.listAllObjects().map { it.key }.toSet()

        for (upload in replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()) {
            val blockIdx = upload.blockIndex

            assertTrue(
                TableCatalog.blockFilePath(blockIdx) in storedPaths,
                "block file missing for b$blockIdx"
            )

            val tables = upload.tries.map { fromSchemaAndTable(it.tableName) }.toSet()

            for (trie in upload.tries) {
                val slug = TableSlug.of(fromSchemaAndTable(trie.tableName))
                assertTrue(
                    slug.dataFilePath(trie.trieKey) in storedPaths,
                    "data file missing for ${trie.tableName}/${trie.trieKey}"
                )
                assertTrue(
                    slug.metaFilePath(trie.trieKey) in storedPaths,
                    "meta file missing for ${trie.tableName}/${trie.trieKey}"
                )
            }

            for (table in tables) {
                assertTrue(
                    TableCatalog.tableBlockPath(TableSlug.of(table), blockIdx) in storedPaths,
                    "table-block file missing for ${table.schemaAndTable}/b$blockIdx"
                )
            }
        }
    }

    /**
     * The replica records every reader applies — the raw log minus what the term fence discards.
     *
     * A superseded leader learns it has lost only by reading the winning claim back, so it goes on
     * appending behind that claim; those records reach the log and every reader folds them out again.
     * The invariants below are about what was applied, so they fold the same way.
     */
    private fun appliedMessages(): List<ReplicaMessage> =
        TermFence(0).let { fence ->
            replicaLog.topic
                .filter { fence.admit(it.message.termId) != TermFence.Admission.FENCED }
                .map { it.message }
        }

    private fun abortedTxIds(): Set<MessageId> =
        appliedMessages()
            .filterIsInstance<ReplicaMessage.ResolvedTx>()
            .filter { !it.committed }
            .map { it.txId }
            .toSet()

    private fun assertSnapshotHasNoAbortedRows(node: SimNode) {
        node.liveIndex.openSnapshot(node.liveIndex.latestCompletedTx?.systemTime).use { snap ->
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
        appliedMessages()
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
        // Distinct because one boundary can be produced by two terms, so an index can be uploaded twice.
        val uploads =
            replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>().map { it.blockIndex }.distinct()
        assertEquals(boundaries, uploads, "every BlockBoundary should have a matching BlockUploaded")
        assertEquals(
            boundaries.indices.map { it.toLong() }, boundaries,
            "block indices should be contiguous starting from 0"
        )
    }

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks across leadership churn`() =
        runTest(timeout = 5.seconds) {
            val rowsPerBlock = rand.nextLong(15, 25)
            val totalActions = rand.nextInt(50, 100)
            val actions = buildActions(rand, totalActions)
            val simExtSource = SimExtSource(actions)
            val srcLogEventCount = rand.nextInt(20, 40)
            LOG.debug("test: $totalActions actions, $srcLogEventCount srcLogEvents (rowsPerBlock=$rowsPerBlock)")

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, IndexerConfig(rowsPerBlock = rowsPerBlock), simExtSource).use { node ->
                    launch(dispatcher) {

                        val logProc = node.openLogProcessor(this)

                        launch {
                            repeat(srcLogEventCount) {
                                yield()
                                if (rand.nextInt(100) < 50) {
                                    // A rival claims one term above whatever this node has seen, which supersedes it; reporting the tip then lets it claim its way back.
                                    replicaLog.appendMessage(NoOp(termId = logProc.termFence.highestSeen + 1))
                                    replicaLog.reportTip()
                                } else {
                                    srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                }
                            }

                            // One report is one election timeout, and this node only wins its leadership back on a poll that came back empty, so the drain needs its own supply.
                            val elections = launch { while (true) { replicaLog.reportTip(); yield() } }
                            simExtSource.awaitQuiescence()
                            elections.cancel()

                            replicaLog.awaitAllDelivered()
                            awaitIdle()
                        }.invokeOnCompletion { cancel() }
                    }.join()

                    assertReplicaTxInvariants()
                    assertEquals(
                        totalActions, replicaTxIds().size,
                        "all actions should appear on the replica"
                    )

                    val replicaMessages = appliedMessages()
                    assertBlockBoundariesMatchUploads(replicaMessages)

                    val expectedBlockIndex =
                        replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()
                            .maxOfOrNull { it.blockIndex }

                    assertEquals(
                        expectedBlockIndex, node.tableCatalog.currentBlockIndex,
                        "block catalog should match latest uploaded block"
                    )

                    val replicaTxIds = replicaTxIds()

                    if (replicaTxIds.isNotEmpty()) {
                        assertEquals(
                            replicaMessages.filterIsInstance<ReplicaMessage.ResolvedTx>().last().txId,
                            node.liveIndex.latestCompletedTx?.txId,
                            "live index latestCompletedTx should match last replica tx"
                        )
                    }

                    assertBlockFilesExist(bp, "test-db", replicaMessages)
                    assertSnapshotHasNoAbortedRows(node)
                }
            }
        }

    @RepeatableSimulationTest
    fun `stable leader with sustained throughput`() =
        runTest(timeout = 5.seconds) {
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
                            launch(dispatcher) {

                                leader.openLogProcessor(this)
                                followerA.openLogProcessor(this)
                                followerB.openLogProcessor(this)

                                launch {
                                    repeat(srcLogEventCount) {
                                        yield()
                                        srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                    }
                                    simExtSource.awaitQuiescence()
                                    replicaLog.awaitAllDelivered()

                                    // Anchor the per-node `latestTxId` to the latest replica tx so the convergence assertions below see consistent state across nodes.
                                    val lastReplicaTxId = appliedMessages()
                                        .filterIsInstance<ReplicaMessage.ResolvedTx>()
                                        .maxOfOrNull { it.txId }
                                    if (lastReplicaTxId != null) {
                                        leader.watchers.awaitTx(lastReplicaTxId)
                                        followerA.watchers.awaitTx(lastReplicaTxId)
                                        followerB.watchers.awaitTx(lastReplicaTxId)
                                    }

                                    awaitIdle()
                                }.invokeOnCompletion { cancel() }
                            }.join()

                            assertReplicaTxInvariants()
                            assertEquals(
                                totalActions, replicaTxIds().size,
                                "all actions should appear on the replica"
                            )

                            val replicaMessages = appliedMessages()
                            assertBlockBoundariesMatchUploads(replicaMessages)

                            val nodes = listOf(leader, followerA, followerB)

                            val expectedBlockIndex =
                                replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()
                                    .maxOfOrNull { it.blockIndex }
                            for (node in nodes) {
                                assertEquals(
                                    expectedBlockIndex, node.tableCatalog.currentBlockIndex,
                                    "block catalog should match latest uploaded block"
                                )
                            }

                            val expectedLatestCompletedTx = leader.liveIndex.latestCompletedTx
                            val expectedTableCatalogTx = leader.tableCatalog.latestCompletedTx
                            val expectedProcessedMsgId = leader.tableCatalog.latestProcessedMsgId

                            for (node in nodes) {
                                assertEquals(
                                    expectedProcessedMsgId, node.tableCatalog.latestProcessedMsgId,
                                    "all nodes should agree on latestProcessedMsgId"
                                )
                                assertEquals(
                                    expectedTableCatalogTx, node.tableCatalog.latestCompletedTx,
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
                        }
                    }
                }
            }
        }

    @RepeatableSimulationTest
    fun `multi-node leadership changes preserve block catalog consistency`() =
        runTest(timeout = 5.seconds) {
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
                        launch(dispatcher) {

                            nodeA.openLogProcessor(this)
                            nodeB.openLogProcessor(this)

                            launch {
                                repeat(srcLogEventCount) {
                                    yield()
                                    if (rand.nextInt(100) < 50) {
                                        // Whichever follower is caught up sees the tip and claims, which supersedes the incumbent — leadership moves without a coordinator.
                                        replicaLog.reportTip()
                                    } else {
                                        srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                    }
                                }
                                simExtSource.awaitQuiescence()
                                replicaLog.awaitAllDelivered()

                                // Anchor the per-node `latestTxId` to the latest replica tx so the convergence assertions below see consistent state across nodes.
                                val lastReplicaTxId = appliedMessages()
                                    .filterIsInstance<ReplicaMessage.ResolvedTx>()
                                    .maxOfOrNull { it.txId }
                                if (lastReplicaTxId != null) {
                                    nodeA.watchers.awaitTx(lastReplicaTxId)
                                    nodeB.watchers.awaitTx(lastReplicaTxId)
                                }

                                awaitIdle()
                            }.invokeOnCompletion { cancel() }
                        }.join()

                        assertReplicaTxInvariants()
                        assertEquals(
                            totalActions, replicaTxIds().size,
                            "all actions should appear on the replica"
                        )

                        val replicaMessages = appliedMessages()
                        assertBlockBoundariesMatchUploads(replicaMessages)

                        val expectedBlockIndex = replicaMessages
                            .filterIsInstance<ReplicaMessage.BlockUploaded>()
                            .maxOfOrNull { it.blockIndex }

                        assertEquals(
                            expectedBlockIndex, nodeA.tableCatalog.currentBlockIndex,
                            "node A block catalog should match latest uploaded block"
                        )
                        assertEquals(
                            expectedBlockIndex, nodeB.tableCatalog.currentBlockIndex,
                            "node B block catalog should match latest uploaded block"
                        )

                        assertEquals(
                            nodeA.tableCatalog.latestProcessedMsgId, nodeB.tableCatalog.latestProcessedMsgId,
                            "both nodes should agree on latestProcessedMsgId"
                        )

                        assertEquals(
                            nodeA.tableCatalog.latestCompletedTx, nodeB.tableCatalog.latestCompletedTx,
                            "both nodes should agree on block catalog's latestCompletedTx"
                        )

                        assertEquals(
                            nodeA.liveIndex.latestCompletedTx, nodeB.liveIndex.latestCompletedTx,
                            "both nodes should agree on live index's latestCompletedTx"
                        )

                        assertBlockFilesExist(bp, "test-db", replicaMessages)

                        assertSnapshotHasNoAbortedRows(nodeA)
                        assertSnapshotHasNoAbortedRows(nodeB)
                    }
                }
            }
        }

    /**
     * A term ending is a role change rather than a fault, so churn poisons no node's watchers
     * (StandingDownLeavesTheDatabaseReadable).
     */
    private fun assertNoNodeFailed(nodes: List<SimNode>) {
        for ((idx, node) in nodes.withIndex())
            assertNull(node.watchers.exception, "node $idx's watchers were poisoned (seed=$currentSeed)")
    }

    /**
     * Only a leader writes anything but a claim, so every non-`NoOp` record stamped with one term came
     * from one node (ExactlyOneConfirmedLeaderPerDatabase).
     *
     * `NoOp` is excluded because a claim is one, and two nodes claiming at the same term is exactly what
     * the log is there to settle: both records land, and position decides which of them confers.
     */
    private fun assertOneWriterPerTerm(nodes: List<SimNode>) {
        val writersByTerm = mutableMapOf<Long, MutableSet<Int>>()

        for (record in replicaLog.topic) {
            if (record.message is NoOp) continue
            val writer = nodes.indexOfFirst { record.logOffset in it.nodeReplicaLog.appendedOffsets }
            if (writer >= 0) writersByTerm.getOrPut(record.message.termId) { mutableSetOf() } += writer
        }

        for ((termId, writers) in writersByTerm)
            assertEquals(
                1, writers.size,
                "term $termId carries records from more than one node: $writers (seed=$currentSeed)"
            )
    }

    private suspend fun List<SimNode>.awaitTx(txId: MessageId?) {
        if (txId != null) forEach { it.watchers.awaitTx(txId) }
    }

    /**
     * How many of these nodes are leading, and MUST be called while the simulation is still up.
     *
     * The driver coroutine cancels the scope every term runs in as it completes, so a node sampled after
     * the join reports itself as not leading whatever it was doing a moment earlier.
     */
    private fun List<SimNode>.leaderCount() = count { it.isLeader }

    /**
     * Two leaders uploading the same block index is fine, the upload being deterministic from the
     * boundary. Two disagreeing about *which* cut block N is is not: they would write the same
     * object-store paths from two different snapshots. The boundary's position isn't on the
     * `BlockUploaded`, but its watermark came from it, so two watermarks under one index are two boundaries.
     */
    private fun assertBlockCutsAgree() {
        val uploads = replicaLog.topic.map { it.message }.filterIsInstance<ReplicaMessage.BlockUploaded>()

        for ((blockIndex, watermarks) in uploads.groupBy({ it.blockIndex }, { it.latestProcessedMsgId }))
            assertEquals(
                1, watermarks.distinct().size,
                "block b$blockIndex was uploaded for more than one boundary: " +
                        "${watermarks.distinct()} (seed=$currentSeed)"
            )
    }

    private fun assertNodesConverged(nodes: List<SimNode>) {
        val expected = nodes.first()

        for ((idx, node) in nodes.withIndex()) {
            assertEquals(
                expected.tableCatalog.currentBlockIndex, node.tableCatalog.currentBlockIndex,
                "node $idx disagrees on the current block index (seed=$currentSeed)"
            )
            assertEquals(
                expected.tableCatalog.latestProcessedMsgId, node.tableCatalog.latestProcessedMsgId,
                "node $idx disagrees on latestProcessedMsgId (seed=$currentSeed)"
            )
            assertEquals(
                expected.liveIndex.latestCompletedTx, node.liveIndex.latestCompletedTx,
                "node $idx disagrees on the live index's latestCompletedTx (seed=$currentSeed)"
            )
        }
    }

    private fun lastAppliedTxId(): MessageId? =
        appliedMessages().filterIsInstance<ReplicaMessage.ResolvedTx>().maxOfOrNull { it.txId }

    @RepeatableSimulationTest
    fun `leadership churn leaves one leader, and each term one writer`() =
        runTest(timeout = 5.seconds) {
            val indexerConfig = IndexerConfig(rowsPerBlock = rand.nextLong(15, 25))
            val totalActions = rand.nextInt(30, 60)
            val simExtSource = SimExtSource(buildActions(rand, totalActions))
            val srcLogEventCount = rand.nextInt(20, 40)

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeA ->
                    SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeB ->
                        SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeC ->
                            val nodes = listOf(nodeA, nodeB, nodeC)
                            var leadersAtQuiescence = -1

                            launch(dispatcher) {
                                nodes.forEach { it.openLogProcessor(this) }

                                launch {
                                    // Three nodes rather than two so that a reported tip usually finds more than one follower caught up, which is what puts two claims at the same term on the log for position to settle.
                                    repeat(srcLogEventCount) { event ->
                                        yield()
                                        val tip = rand.nextInt(100) < 50
                                        // The first event always reports the tip, so a run whose draws
                                        // never do still gives the followers one chance to claim.
                                        if (event == 0 || tip) replicaLog.reportTip()
                                        else srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                    }

                                    simExtSource.awaitQuiescence()
                                    replicaLog.awaitAllDelivered()
                                    nodes.awaitTx(lastAppliedTxId())
                                    awaitIdle()

                                    leadersAtQuiescence = nodes.leaderCount()
                                }.invokeOnCompletion { cancel() }
                            }.join()

                            assertReplicaTxInvariants()
                            assertEquals(
                                totalActions, replicaTxIds().size,
                                "all actions should appear on the replica"
                            )

                            assertOneWriterPerTerm(nodes)
                            assertNoNodeFailed(nodes)
                            assertNodesConverged(nodes)
                            assertBlockCutsAgree()

                            assertEquals(
                                1, leadersAtQuiescence,
                                "churn should settle on exactly one leader (seed=$currentSeed)"
                            )
                        }
                    }
                }
            }
        }

    @RepeatableSimulationTest
    fun `a read-only node follows the churn and writes nothing`() =
        runTest(timeout = 5.seconds) {
            val indexerConfig = IndexerConfig(rowsPerBlock = rand.nextLong(15, 25))
            val totalActions = rand.nextInt(30, 60)
            val simExtSource = SimExtSource(buildActions(rand, totalActions))
            val srcLogEventCount = rand.nextInt(20, 40)

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeA ->
                    SimNode("test-db", bp, indexerConfig, simExtSource).use { nodeB ->
                        SimNode("test-db", bp, indexerConfig, simExtSource, readOnly = true).use { readOnly ->
                            val nodes = listOf(nodeA, nodeB, readOnly)
                            var readOnlyLed = false

                            launch(dispatcher) {
                                nodes.forEach { it.openLogProcessor(this) }

                                launch {
                                    repeat(srcLogEventCount) {
                                        yield()
                                        // Sampled every step, not once at the end: a term it should never
                                        // have held could begin and be superseded between two of these.
                                        readOnlyLed = readOnlyLed || readOnly.isLeader
                                        if (rand.nextInt(100) < 50) replicaLog.reportTip()
                                        else srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                    }

                                    simExtSource.awaitQuiescence()
                                    replicaLog.awaitAllDelivered()
                                    nodes.awaitTx(lastAppliedTxId())
                                    awaitIdle()

                                    readOnlyLed = readOnlyLed || readOnly.isLeader
                                }.invokeOnCompletion { cancel() }
                            }.join()

                            assertFalse(
                                readOnlyLed,
                                "a read-only node must never lead (seed=$currentSeed)"
                            )
                            assertEquals(
                                emptySet<LogOffset>(), readOnly.nodeReplicaLog.appendedOffsets,
                                "a read-only node must put nothing on the replica log (seed=$currentSeed)"
                            )

                            assertNodesConverged(nodes)
                            assertNoNodeFailed(nodes)
                            assertSnapshotHasNoAbortedRows(readOnly)
                        }
                    }
                }
            }
        }

    @RepeatableSimulationTest
    fun `a node joining a live database catches up without unseating its leader`() =
        runTest(timeout = 5.seconds) {
            val indexerConfig = IndexerConfig(rowsPerBlock = rand.nextLong(15, 25))
            val totalActions = rand.nextInt(30, 60)
            val simExtSource = SimExtSource(buildActions(rand, totalActions))
            val srcLogEventCount = rand.nextInt(5, 15)

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, indexerConfig, simExtSource).use { incumbent ->
                    SimNode("test-db", bp, indexerConfig, simExtSource).use { joiner ->
                        val nodes = listOf(incumbent, joiner)
                        var leadersAtQuiescence = -1
                        var joinerLed = false

                        launch(dispatcher) {
                            val nodeScope = this
                            incumbent.openLogProcessor(nodeScope)

                            launch {
                                repeat(srcLogEventCount) {
                                    yield()
                                    srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                }

                                simExtSource.awaitQuiescence()
                                replicaLog.awaitAllDelivered()
                                awaitIdle()

                                joiner.openLogProcessor(nodeScope)

                                nodes.awaitTx(lastAppliedTxId())
                                replicaLog.awaitAllDelivered()
                                awaitIdle()

                                leadersAtQuiescence = nodes.leaderCount()
                                joinerLed = joiner.isLeader
                            }.invokeOnCompletion { cancel() }
                        }.join()

                        assertFalse(
                            joinerLed,
                            "a claim behind a term already on the log confers nothing (seed=$currentSeed)"
                        )
                        assertEquals(
                            1, leadersAtQuiescence,
                            "a node joining must not unseat the leader (seed=$currentSeed)"
                        )

                        assertReplicaTxInvariants()
                        assertNodesConverged(nodes)
                        assertNoNodeFailed(nodes)
                        assertSnapshotHasNoAbortedRows(joiner)
                    }
                }
            }
        }
}
