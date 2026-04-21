package xtdb.indexer

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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
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
import xtdb.storage.MemoryStorage
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.util.debug
import xtdb.util.logger
import java.time.Instant
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

    private fun simIndexer(liveIndex: LiveIndex, dbName: String) = object : Indexer.ForDatabase {

        private fun openAndCommit(txKey: TransactionKey, committed: Boolean): Pair<Map<String, ByteArray>, ReplicaMessage.ResolvedTx> {
            val openTx = liveIndex.startTx(txKey)
            with(Indexer) { openTx.addTxRow(dbName, txKey, if (committed) null else RuntimeException("aborted")) }
            val tableData = openTx.serializeTableData()
            liveIndex.commitTx(openTx)
            openTx.close()
            return tableData to ReplicaMessage.ResolvedTx(
                txId = txKey.txId,
                systemTime = txKey.systemTime,
                committed = committed,
                error = null,
                tableData = tableData
            )
        }

        override fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: xtdb.arrow.VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?, userMetadata: Any?
        ): ReplicaMessage.ResolvedTx {
            val txKey = TransactionKey(msgId, systemTime ?: msgTimestamp)
            return openAndCommit(txKey, committed = rand.nextFloat() > 0.3f).second
        }

        override fun addTxRow(txKey: TransactionKey, error: Throwable?): ReplicaMessage.ResolvedTx =
            openAndCommit(txKey, committed = error == null).second

        override fun close() {}
    }

    private inner class SimNode(
        val dbName: String, val bp: MemoryStorage, indexerConfig: IndexerConfig,
    ) : LogProcessor.ProcessorFactory, AutoCloseable {

        val blockCatalog = BlockCatalog(dbName, null)
        val tableCatalog = TableCatalog(bp)
        val trieCatalog = createTrieCatalog()
        val liveIndex = LiveIndex.open(allocator, blockCatalog, tableCatalog, dbName, indexerConfig)

        val dbState = DatabaseState(dbName, blockCatalog, tableCatalog, trieCatalog, liveIndex)

        val watchers = Watchers(-1)
        private val indexer = simIndexer(liveIndex, dbName)
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
            dbState.close()
        }
    }

    private fun assertBlockFilesExist(bp: MemoryStorage, dbName: String, replicaMessages: List<ReplicaMessage>) {
        val storedPaths = bp.listAllObjects().map { it.key }.toSet()

        for (upload in replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>()) {
            val blockIdx = upload.blockIndex

            assertTrue(BlockCatalog.blockFilePath(blockIdx) in storedPaths,
                "block file missing for b$blockIdx")

            val tables = upload.tries.map { TableRef.parse(dbName, it.tableName) }.toSet()

            for (trie in upload.tries) {
                val table = TableRef.parse(dbName, trie.tableName)
                assertTrue(table.dataFilePath(trie.trieKey) in storedPaths,
                    "data file missing for ${trie.tableName}/${trie.trieKey}")
                assertTrue(table.metaFilePath(trie.trieKey) in storedPaths,
                    "meta file missing for ${trie.tableName}/${trie.trieKey}")
            }

            for (table in tables) {
                assertTrue(BlockCatalog.tableBlockPath(table, blockIdx) in storedPaths,
                    "table-block file missing for ${table.schemaAndTable}/b$blockIdx")
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

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks with rebalances`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val bp = MemoryStorage(allocator, epoch = 0)
            val rowsPerBlock = rand.nextLong(3, 10)
            val node = SimNode("test-db", bp, IndexerConfig(rowsPerBlock = rowsPerBlock))

            val logProcScope = CoroutineScope(dispatcher + Job())
            node.openLogProcessor(logProcScope).use { logProc ->
                val groupJob = launch(dispatcher) { srcLog.openGroupSubscription(logProc) }

                launch(dispatcher) {
                    val totalActions = rand.nextInt(5, 20)
                    LOG.debug("test: will perform $totalActions actions (rowsPerBlock=$rowsPerBlock)")
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

                val expectedBlockIndex = uploads.maxOfOrNull { it }
                assertEquals(expectedBlockIndex, node.blockCatalog.currentBlockIndex,
                    "block catalog should match latest uploaded block")

                if (replicaTxIds.isNotEmpty()) {
                    val lastReplicaTx = replicaMessages
                        .filterIsInstance<ReplicaMessage.ResolvedTx>().last()
                    assertEquals(lastReplicaTx.txId, node.liveIndex.latestCompletedTx?.txId,
                        "live index latestCompletedTx should match last replica tx")
                }

                assertBlockFilesExist(bp, "test-db", replicaMessages)
            }

            node.close()
            bp.close()
        }

    @RepeatableSimulationTest
    fun `multi-node leadership changes preserve block catalog consistency`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            val bp = MemoryStorage(allocator, epoch = 0)
            val rowsPerBlock = rand.nextLong(3, 10)
            val indexerConfig = IndexerConfig(rowsPerBlock = rowsPerBlock)
            val nodeA = SimNode("test-db", bp, indexerConfig)
            val nodeB = SimNode("test-db", bp, indexerConfig)

            val scopeA = CoroutineScope(dispatcher + Job())
            val scopeB = CoroutineScope(dispatcher + Job())

            nodeA.openLogProcessor(scopeA).use { logProcA ->
                nodeB.openLogProcessor(scopeB).use { logProcB ->
                    val groupJobA = launch(dispatcher) { srcLog.openGroupSubscription(logProcA) }
                    val groupJobB = launch(dispatcher) { srcLog.openGroupSubscription(logProcB) }

                    launch(dispatcher) {
                        val totalActions = rand.nextInt(5, 20)
                        LOG.debug("test: multi-node will perform $totalActions actions (rowsPerBlock=$rowsPerBlock)")
                        repeat(totalActions) { _ ->
                            yield()
                            when (rand.nextInt(3)) {
                                0 -> srcLog.appendMessage(emptyTx())
                                1 -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                2 -> srcLog.rebalanceTrigger.send(Unit)
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

                    assertEquals(sourceTxIds, replicaTxIds, "every source tx should appear on the replica, in order")
                    assertEquals(replicaTxIds, replicaTxIds.sorted(), "replica txIds should be monotonically increasing")
                    assertEquals(replicaTxIds.size, replicaTxIds.toSet().size, "replica should have no duplicate txIds")

                    val replicaMessages = replicaLog.topic.map { it.message }
                    val boundaries = replicaMessages.filterIsInstance<ReplicaMessage.BlockBoundary>().map { it.blockIndex }
                    val uploads = replicaMessages.filterIsInstance<ReplicaMessage.BlockUploaded>().map { it.blockIndex }
                    assertEquals(boundaries, uploads, "every BlockBoundary should have a matching BlockUploaded")

                    assertEquals(boundaries.indices.map { it.toLong() }, boundaries,
                        "block indices should be contiguous starting from 0")

                    val expectedBlockIndex = replicaMessages
                        .filterIsInstance<ReplicaMessage.BlockUploaded>()
                        .maxOfOrNull { it.blockIndex }

                    assertEquals(expectedBlockIndex, nodeA.blockCatalog.currentBlockIndex,
                        "node A block catalog should match latest uploaded block")
                    assertEquals(expectedBlockIndex, nodeB.blockCatalog.currentBlockIndex,
                        "node B block catalog should match latest uploaded block")

                    assertEquals(nodeA.watchers.latestSourceMsgId, nodeB.watchers.latestSourceMsgId,
                        "both nodes should converge on the same source watermark")

                    assertEquals(nodeA.blockCatalog.latestProcessedMsgId, nodeB.blockCatalog.latestProcessedMsgId,
                        "both nodes should agree on latestProcessedMsgId")

                    assertEquals(nodeA.blockCatalog.latestCompletedTx, nodeB.blockCatalog.latestCompletedTx,
                        "both nodes should agree on block catalog's latestCompletedTx")

                    assertEquals(nodeA.liveIndex.latestCompletedTx, nodeB.liveIndex.latestCompletedTx,
                        "both nodes should agree on live index's latestCompletedTx")

                    assertBlockFilesExist(bp, "test-db", replicaMessages)
                }
            }

            nodeA.close()
            nodeB.close()
            bp.close()
        }
}
