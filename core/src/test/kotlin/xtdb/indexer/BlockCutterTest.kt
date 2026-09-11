@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import io.mockk.mockk
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.InMemoryLog
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.BlockUploaded
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.tx.OpenTx
import xtdb.catalog.TableCatalog
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.MemoryStorage
import xtdb.trie.Trie
import xtdb.util.closeAll
import java.time.Instant
import java.time.InstantSource
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * The leader's block cycle: what the boundary a cut emits carries, what the upload behind it writes, and
 * what the cutter will take in between.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal class BlockCutterTest {

    private val table = TableRef("public", "docs")

    private lateinit var allocator: RootAllocator
    private lateinit var nodeBase: NodeBase
    private val toClose = mutableListOf<AutoCloseable>()

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        nodeBase = openBase(openMeterRegistry = false)
    }

    @AfterEach
    fun tearDown() {
        toClose.closeAll()
        nodeBase.close()
        allocator.close()
    }

    /**
     * A real live index, storage and catalogs behind the cutter, so an upload writes the tries and block
     * files it would in production and the row gauge counts rows that were actually applied.
     *
     * The cutter is built by [cutter] rather than here, because it seeds its gauge from the live index at
     * construction — a test of that seeding has to apply its rows first.
     */
    private inner class Term(
        blockThreshold: Long,
        private val ioDispatcher: CoroutineDispatcher,
    ) : AutoCloseable {
        private val bufferPool = MemoryStorage(allocator, epoch = 0)
        val tableCatalog = TableCatalog(bufferPool)
        val trieCatalog = createTrieCatalog()

        val liveIndex = LiveIndex.open(
            allocator, tableCatalog, trieCatalog,
            IndexerConfig().rowsPerBlock(blockThreshold), ioDispatcher
        )

        private val partitionState = PartitionState(tableCatalog, trieCatalog, liveIndex)
        private val partitionStorage = PartitionStorage(
            DatabaseLogs(
                InMemoryLog<SourceMessage>(InstantSource.system(), 0),
                InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
            ),
            bufferPool, null
        )

        private val driver = RecordingLogsDriver()
        val appender = ReplicaLogAppender(driver, leaderTerm = 1, NoAssertElectionDriver)

        val appended get() = driver.appended.toList()

        /** Apply a tx of [rows] rows into the open block, as the consume-back does. */
        fun applyRows(txId: Long, rows: Int) {
            OpenTx(
                allocator, nodeBase, partitionStorage, partitionState, "test",
                TransactionKey(txId, Instant.EPOCH), null
            ).use { tx ->
                repeat(rows) {
                    tx.table(table).apply {
                        writeId(UUID.randomUUID())
                        writeValidTimeMicros(0, 0)
                        putDocWriter.endStruct()
                        endPut()
                    }
                }
                tx.writeTxRow(null, null)
                liveIndex.commitTx(tx.txKey, tx.tables.associate { (ref, t) -> ref to t.txRelation })
            }
        }

        fun cutter(scope: CoroutineScope) =
            BlockCutter(
                partitionStorage, partitionState, "test", leaderTerm = 1, replicaAppender = appender,
                logsDriver = driver, compactor = mockk(relaxed = true), dbCatalog = null,
                meterRegistry = null, lastUploadEpochSeconds = AtomicLong(0), scope = scope,
                ioDispatcher = ioDispatcher
            )

        override fun close() {
            partitionState.close()
            bufferPool.close()
        }
    }

    /**
     * Both messages of a block cut reach the same driver, so it holds the whole of what the cycle emits —
     * but by different routes, which is why only one of them needs the scheduler. The boundary is queued
     * on the pump, which appends from its own coroutine, so it lands on the next `runCurrent`; the upload
     * is appended on the caller's, so it is there the moment [BlockCutter.upload] returns.
     */
    private fun TestScope.term(blockThreshold: Long = IndexerConfig().rowsPerBlock) =
        Term(blockThreshold, StandardTestDispatcher(testScheduler))
            .also {
                toClose += it
                backgroundScope.launch { it.appender.run() }
            }

    @Test
    fun `a cut emits the boundary, and the upload behind it the matching BlockUploaded`() = runTest {
        val term = term()
        val cutter = term.cutter(backgroundScope)
        val token = byteArrayOf(1, 2, 3)

        term.applyRows(txId = 0, rows = 1)

        cutter.cut(latestProcessedMsgId = 7, extToken = token)
        runCurrent()

        val boundary = assertInstanceOf(BlockBoundary::class.java, term.appended.single())
        assertEquals(0, boundary.blockIndex, "the first block a database cuts is block 0")
        assertEquals(7, boundary.latestProcessedMsgId)
        assertArrayEquals(token, boundary.externalSourceToken)

        cutter.upload(PendingBlock(boundaryMsgId = 0, boundaryMessage = boundary))
        runCurrent()

        val uploaded = assertInstanceOf(BlockUploaded::class.java, term.appended.last())
        assertEquals(0, uploaded.blockIndex)
        assertEquals(7, uploaded.latestProcessedMsgId)
        assertArrayEquals(token, uploaded.externalSourceToken)
        assertEquals(
            listOf(Trie.l0Key(0).toString()),
            uploaded.tries.filter { it.tableName == "public/docs" }.map { it.trieKey },
            "the L0 this block wrote for our table, so a follower can pick it up"
        )
    }

    @Test
    fun `nothing resolves between a cut and the upload reading back`() = runTest {
        val term = term()
        val cutter = term.cutter(backgroundScope)

        assertTrue(cutter.acceptingResolution)

        cutter.cut(latestProcessedMsgId = 0, extToken = null)
        runCurrent()

        assertFalse(cutter.acceptingResolution, "resolution is refused from the cut")
        assertThrows<IllegalStateException> { cutter.addRows(1) }

        cutter.upload(PendingBlock(0, term.appended.single() as BlockBoundary))
        runCurrent()

        assertFalse(
            cutter.acceptingResolution,
            "and still refused once produced: the live index is holding a block already snapshotted into L0"
        )
        assertThrows<IllegalStateException> { cutter.addRows(1) }

        cutter.closeBlock(term.appended.last() as BlockUploaded, Instant.EPOCH)
        assertTrue(cutter.acceptingResolution, "the read-back re-opens the block behind it")
    }

    @Test
    fun `the catalog and the live index move on the read-back, not on the upload`() = runTest {
        val term = term()
        val cutter = term.cutter(backgroundScope)

        term.applyRows(txId = 0, rows = 1)
        assertEquals(2, term.liveIndex.blockRowCount, "the put, and the tx's own row in the txs table")

        cutter.cut(latestProcessedMsgId = 0, extToken = null)
        runCurrent()
        cutter.upload(PendingBlock(0, term.appended.single() as BlockBoundary))
        runCurrent()

        assertNull(
            term.tableCatalog.currentBlockIndex,
            "the block file has landed, but this node hasn't adopted it — so the block stays re-producible"
        )
        assertNull(term.tableCatalog.rowCount(table), "nothing folded into the table catalog either")
        assertEquals(emptySet<TableRef>(), term.trieCatalog.tables, "nor the block's L0 tries registered")
        assertEquals(2, term.liveIndex.blockRowCount, "and the rows are still the open block's")

        val held = cutter.closeBlock(term.appended.last() as BlockUploaded, Instant.EPOCH)

        assertEquals(0, term.tableCatalog.currentBlockIndex)
        assertEquals(1, term.tableCatalog.rowCount(table))
        assertEquals(listOf("l00-rc-b00"), term.trieCatalog.listAllTrieKeys(table))
        assertEquals(0, term.liveIndex.blockRowCount)
        assertTrue(held.bufferedRecords.isEmpty(), "a block this term cut itself holds nothing back")
    }

    @Test
    fun `a block produced twice before it is adopted folds its row count once`() = runTest {
        val term = term()

        term.applyRows(txId = 0, rows = 1)

        term.cutter(backgroundScope).let { dyingTerm ->
            dyingTerm.cut(latestProcessedMsgId = 0, extToken = null)
            runCurrent()
            dyingTerm.upload(PendingBlock(0, term.appended.single() as BlockBoundary))
            runCurrent()
        }

        val boundary = term.appended.first { it is BlockBoundary } as BlockBoundary
        val nextTerm = term.cutter(backgroundScope)
        nextTerm.upload(PendingBlock(0, boundary))
        runCurrent()

        nextTerm.closeBlock(term.appended.last { it is BlockUploaded } as BlockUploaded, Instant.EPOCH)

        assertEquals(1, term.tableCatalog.rowCount(table))
    }

    @Test
    fun `an empty block is never full, whatever the threshold`() = runTest {
        val cutter = term(blockThreshold = 0).cutter(backgroundScope)

        assertFalse(cutter.isFull, "cutting an empty block would live-lock the term")

        cutter.addRows(1)
        assertTrue(cutter.isFull)
    }

    @Test
    fun `a block inherited part-filled is full where the previous leader would have cut it`() = runTest {
        val term = term(blockThreshold = 10)

        // The rows a previous leader had already applied into the open block, which replay leaves behind.
        // Eight puts, because the tx's own row in `xt$txs` counts towards the block like any other.
        term.applyRows(txId = 0, rows = 8)
        assertEquals(9, term.liveIndex.blockRowCount, "one short of the threshold")

        val cutter = term.cutter(backgroundScope)
        assertFalse(cutter.isFull)

        cutter.addRows(1)
        assertTrue(cutter.isFull, "the gauge is seeded from the rows already in the open block")
    }
}
