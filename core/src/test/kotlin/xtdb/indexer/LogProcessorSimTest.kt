package xtdb.indexer

import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
import xtdb.api.TransactionKey
import xtdb.api.log.*
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.error.Incorrect
import xtdb.storage.MemoryStorage
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.util.asIid
import xtdb.util.debug
import xtdb.util.logger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId
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
        this.srcLog = SimLog("src", dispatcher, rand)
        this.replicaLog = SimLog("replica", dispatcher, rand)
    }

    @AfterEach
    fun tearDown() {
        replicaLog.close()
        srcLog.close()
        allocator.close()
        nodeBase.close()
    }

    private val docsTable = TableRef("test-db", "public", "docs")

    private fun simIndexer(al: BufferAllocator, dbName: String, dbStorage: DatabaseStorage, dbState: DatabaseState) =
        object : Indexer.ForDatabase {
            private fun commitTx(openTx: OpenTx, txKey: TransactionKey, committed: Boolean): ReplicaMessage.ResolvedTx {
                with(Indexer) { openTx.addTxRow(dbName, txKey, if (committed) null else Incorrect("aborted")) }
                val tableData = openTx.serializeTableData()
                dbState.liveIndex.commitTx(openTx)
                openTx.close()
                return ReplicaMessage.ResolvedTx(
                    txId = txKey.txId,
                    systemTime = txKey.systemTime,
                    committed = committed,
                    error = null,
                    tableData = tableData
                )
            }

            override fun indexTx(
                msgId: MessageId, msgTimestamp: Instant, txOps: VectorReader?,
                systemTime: Instant?, defaultTz: ZoneId?, user: String?, userMetadata: Any?
            ): ReplicaMessage.ResolvedTx {
                val txKey = TransactionKey(msgId, systemTime ?: msgTimestamp)
                val committed = rand.nextFloat() > 0.1f
                val openTx = OpenTx(al, nodeBase, dbStorage, dbState, txKey, externalSourceToken = null)

                if (committed) {
                    val table = openTx.table(docsTable)
                    val rowCount = rand.nextInt(1, 6)
                    repeat(rowCount) {
                        val id = java.util.UUID(rand.nextLong(), rand.nextLong())
                        table.logPut(
                            ByteBuffer.wrap(id.asIid),
                            txKey.systemTime.asMicros,
                            Long.MAX_VALUE
                        ) {
                            table.docWriter.vectorFor("_id", VectorType.UUID.arrowType, false).writeObject(id)
                            table.docWriter.vectorFor("tx_id", VectorType.I64.arrowType, false).writeLong(msgId)
                            table.docWriter.endStruct()
                        }
                    }
                }

                return commitTx(openTx, txKey, committed)
            }

            override fun addTxRow(txKey: TransactionKey, error: Throwable?): ReplicaMessage.ResolvedTx {
                val openTx = OpenTx(al, nodeBase, dbStorage, dbState, txKey, externalSourceToken = null)
                return commitTx(openTx, txKey, committed = error == null)
            }

            override fun close() {}
        }

    private fun simIndexerWrapper(dbName: String, dbStorage: DatabaseStorage) = object : Indexer {
        override fun openForDatabase(
            allocator: BufferAllocator, state: DatabaseState,
            liveIndex: LiveIndex, crashLogger: CrashLogger, txIndexer: TxIndexer,
        ) = simIndexer(allocator, dbName, dbStorage, state)

        override fun close() {}
    }

    private inner class SimNode(
        dbName: String, val bp: MemoryStorage, indexerConfig: IndexerConfig,
    ) : LogProcessor.ProcessorFactory, AutoCloseable {

        val blockCatalog = BlockCatalog(dbName, null)
        val tableCatalog = TableCatalog(bp)
        val trieCatalog = createTrieCatalog()
        val liveIndex = LiveIndex.open(allocator, blockCatalog, tableCatalog, dbName, indexerConfig)

        val dbState = DatabaseState(dbName, blockCatalog, tableCatalog, trieCatalog, liveIndex)

        val watchers = Watchers(-1)
        val dbStorage = DatabaseStorage(srcLog, replicaLog, bp, null)
        val blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
        val crashLogger = CrashLogger(allocator, bp, "sim-node")
        val indexer = simIndexerWrapper(dbName, dbStorage)

        override fun openLeaderSystem(
            replicaProducer: Log.AtomicProducer<ReplicaMessage>,
            afterSourceMsgId: MessageId,
            afterReplicaMsgId: MessageId,
        ): LogProcessor.LeaderSystem {
            val proc = LeaderLogProcessor(
                allocator, nodeBase, dbStorage, replicaProducer,
                dbState, indexer, crashLogger, watchers,
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

    private fun emptyTx(): SourceMessage.Tx =
        SourceMessage.Tx(
            txOps = emptyList<TxOp>().toArrowBytes(allocator),
            systemTime = null,
            defaultTz = ZoneId.of("UTC"),
            user = null,
            userMetadata = null
        )

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

            val tableSnap = snap.table(docsTable) ?: return
            val rel = tableSnap.liveRelation ?: return
            val op = rel["op"]
            val put = op.vectorForOrNull("put") ?: return
            val txIdVec = put.vectorFor("tx_id")
            val basisTxId = snap.txBasis?.txId ?: -1L

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

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks with rebalances`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val rowsPerBlock = rand.nextLong(15, 25)
            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, IndexerConfig(rowsPerBlock = rowsPerBlock)).use { node ->
                    val logProcScope = CoroutineScope(dispatcher + Job())
                    node.openLogProcessor(logProcScope).use { logProc ->
                        val groupJob = launch(dispatcher) { srcLog.openGroupSubscription(logProc) }

                        launch(dispatcher) {
                            val totalActions = rand.nextInt(50, 100)
                            LOG.debug("test: will perform $totalActions actions (rowsPerBlock=$rowsPerBlock)")
                            repeat(totalActions) { _ ->
                                yield()

                                when (rand.nextInt(100)) {
                                    in 0..<80 -> srcLog.appendMessage(emptyTx())
                                    in 80..<95 -> srcLog.rebalanceTrigger.send(Unit)
                                    else -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
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

                        assertEquals(
                            sourceTxIds,
                            replicaTxIds,
                            "every source tx should appear on the replica, in order"
                        )
                        assertEquals(
                            replicaTxIds,
                            replicaTxIds.sorted(),
                            "replica txIds should be monotonically increasing"
                        )
                        assertEquals(
                            replicaTxIds.size,
                            replicaTxIds.toSet().size,
                            "replica should have no duplicate txIds"
                        )

                        val replicaMessages = replicaLog.topic.map { it.message }
                        val boundaries =
                            replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>().map { it.blockIndex }
                        val uploads =
                            replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>().map { it.blockIndex }
                        assertEquals(boundaries, uploads, "every BlockBoundary should have a matching BlockUploaded")

                        assertEquals(
                            boundaries.indices.map { it.toLong() }, boundaries,
                            "block indices should be contiguous starting from 0"
                        )

                        val expectedBlockIndex = uploads.maxOfOrNull { it }
                        assertEquals(
                            expectedBlockIndex, node.blockCatalog.currentBlockIndex,
                            "block catalog should match latest uploaded block"
                        )

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
                    }
                }
            }
        }

    @RepeatableSimulationTest
    fun `stable leader with sustained throughput`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val rowsPerBlock = rand.nextLong(15, 25)
            val indexerConfig = IndexerConfig(rowsPerBlock = rowsPerBlock)

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, indexerConfig).use { leader ->
                    SimNode("test-db", bp, indexerConfig).use { followerA ->
                        SimNode("test-db", bp, indexerConfig).use { followerB ->
                            val leaderScope = CoroutineScope(dispatcher + Job())
                            val followerScopeA = CoroutineScope(dispatcher + Job())
                            val followerScopeB = CoroutineScope(dispatcher + Job())

                            leader.openLogProcessor(leaderScope).use { leaderProc ->
                                followerA.openLogProcessor(followerScopeA).use { followerProcA ->
                                    followerB.openLogProcessor(followerScopeB).use { followerProcB ->
                                        val groupJobLeader =
                                            launch(dispatcher) { srcLog.openGroupSubscription(leaderProc) }
                                        val groupJobA =
                                            launch(dispatcher) { srcLog.openGroupSubscription(followerProcA) }
                                        val groupJobB =
                                            launch(dispatcher) { srcLog.openGroupSubscription(followerProcB) }

                                        launch(dispatcher) {
                                            val totalTxs = rand.nextInt(50, 100)
                                            LOG.debug("test: stable-leader will perform $totalTxs txs (rowsPerBlock=$rowsPerBlock)")
                                            repeat(totalTxs) { _ ->
                                                yield()

                                                when (rand.nextInt(100)) {
                                                    in 0..<95 -> srcLog.appendMessage(emptyTx())
                                                    else -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                                }
                                            }

                                            srcLog.appendMessage(emptyTx())
                                            val lastSrcMsgId = srcLog.latestSubmittedMsgId
                                            if (lastSrcMsgId >= 0) {
                                                leader.watchers.awaitSource(lastSrcMsgId)
                                                followerA.watchers.awaitSource(lastSrcMsgId)
                                                followerB.watchers.awaitSource(lastSrcMsgId)
                                            }

                                            replicaLog.awaitAllDelivered()
                                        }.join()

                                        groupJobLeader.cancel()
                                        groupJobA.cancel()
                                        groupJobB.cancel()
                                        leaderScope.cancel()
                                        followerScopeA.cancel()
                                        followerScopeB.cancel()

                                        val sourceTxIds = srcLog.topic
                                            .filter { it.message is SourceMessage.Tx || it.message is SourceMessage.LegacyTx }
                                            .map { it.msgId }

                                        val replicaTxIds = replicaLog.topic
                                            .map { it.message }
                                            .filterIsInstance<ReplicaMessage.ResolvedTx>()
                                            .map { it.txId }

                                        assertEquals(
                                            sourceTxIds,
                                            replicaTxIds,
                                            "every source tx should appear on the replica, in order"
                                        )
                                        assertEquals(
                                            replicaTxIds,
                                            replicaTxIds.sorted(),
                                            "replica txIds should be monotonically increasing"
                                        )
                                        assertEquals(
                                            replicaTxIds.size,
                                            replicaTxIds.toSet().size,
                                            "replica should have no duplicate txIds"
                                        )

                                        val replicaMessages = replicaLog.topic.map { it.message }
                                        val boundaries =
                                            replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>()
                                                .map { it.blockIndex }
                                        val uploads = replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()
                                            .map { it.blockIndex }
                                        assertEquals(
                                            boundaries,
                                            uploads,
                                            "every BlockBoundary should have a matching BlockUploaded"
                                        )

                                        assertEquals(
                                            boundaries.indices.map { it.toLong() }, boundaries,
                                            "block indices should be contiguous starting from 0"
                                        )

                                        val nodes = listOf(leader, followerA, followerB)

                                        val expectedBlockIndex = uploads.maxOfOrNull { it }
                                        for (node in nodes) {
                                            assertEquals(
                                                expectedBlockIndex, node.blockCatalog.currentBlockIndex,
                                                "block catalog should match latest uploaded block"
                                            )
                                        }

                                        val expectedLatestCompletedTx = leader.liveIndex.latestCompletedTx
                                        val expectedBlockCatalogTx = leader.blockCatalog.latestCompletedTx
                                        val expectedProcessedMsgId = leader.blockCatalog.latestProcessedMsgId
                                        val expectedSourceWatermark = leader.watchers.latestSourceMsgId

                                        for (node in nodes) {
                                            assertEquals(
                                                expectedSourceWatermark, node.watchers.latestSourceMsgId,
                                                "all nodes should converge on the same source watermark"
                                            )
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
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    @RepeatableSimulationTest
    fun `multi-node leadership changes preserve block catalog consistency`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val rowsPerBlock = rand.nextLong(15, 25)
            val indexerConfig = IndexerConfig(rowsPerBlock = rowsPerBlock)

            MemoryStorage(allocator, epoch = 0).use { bp ->
                SimNode("test-db", bp, indexerConfig).use { nodeA ->
                    SimNode("test-db", bp, indexerConfig).use { nodeB ->
                        val scopeA = CoroutineScope(dispatcher + Job())
                        val scopeB = CoroutineScope(dispatcher + Job())

                        nodeA.openLogProcessor(scopeA).use { logProcA ->
                            nodeB.openLogProcessor(scopeB).use { logProcB ->
                                val groupJobA = launch(dispatcher) { srcLog.openGroupSubscription(logProcA) }
                                val groupJobB = launch(dispatcher) { srcLog.openGroupSubscription(logProcB) }

                                launch(dispatcher) {
                                    val totalActions = rand.nextInt(50, 100)
                                    LOG.debug("test: multi-node will perform $totalActions actions (rowsPerBlock=$rowsPerBlock)")
                                    repeat(totalActions) { _ ->
                                        yield()
                                        when (rand.nextInt(100)) {
                                            in 0..<80 -> srcLog.appendMessage(emptyTx())
                                            in 80..<95 -> srcLog.rebalanceTrigger.send(Unit)
                                            else -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                        }
                                    }

                                    srcLog.appendMessage(emptyTx())
                                    val lastSrcMsgId = srcLog.latestSubmittedMsgId
                                    if (lastSrcMsgId >= 0) {
                                        nodeA.watchers.awaitSource(lastSrcMsgId)
                                        nodeB.watchers.awaitSource(lastSrcMsgId)
                                    }

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

                                assertEquals(
                                    sourceTxIds,
                                    replicaTxIds,
                                    "every source tx should appear on the replica, in order"
                                )
                                assertEquals(
                                    replicaTxIds,
                                    replicaTxIds.sorted(),
                                    "replica txIds should be monotonically increasing"
                                )
                                assertEquals(
                                    replicaTxIds.size,
                                    replicaTxIds.toSet().size,
                                    "replica should have no duplicate txIds"
                                )

                                val replicaMessages = replicaLog.topic.map { it.message }
                                val boundaries = replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>()
                                    .map { it.blockIndex }
                                val uploads = replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()
                                    .map { it.blockIndex }
                                assertEquals(
                                    boundaries,
                                    uploads,
                                    "every BlockBoundary should have a matching BlockUploaded"
                                )

                                assertEquals(
                                    boundaries.indices.map { it.toLong() }, boundaries,
                                    "block indices should be contiguous starting from 0"
                                )

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
                                    nodeA.watchers.latestSourceMsgId, nodeB.watchers.latestSourceMsgId,
                                    "both nodes should converge on the same source watermark"
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
                            }
                        }
                    }
                }
            }
        }
}
