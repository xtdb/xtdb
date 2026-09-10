package xtdb.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.TableRef
import xtdb.api.IndexerConfig
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.BlockUploaded
import xtdb.api.log.SourceMessage
import xtdb.catalog.TableCatalog
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.log.proto.trieMetadata
import xtdb.storage.BufferPool
import xtdb.table.fromSchemaAndTable
import java.time.Instant
import java.time.InstantSource
import java.util.concurrent.atomic.AtomicLong

/**
 * The leader's block cycle: what the boundary a cut emits carries, what the upload behind it writes, and
 * what the cutter will take in between.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal class BlockCutterTest {

    private val tableRef = fromSchemaAndTable("public/foo")

    private val finishedBlock = LiveTable.FinishedBlock(
        vecTypes = emptyMap(),
        rowCount = 10,
        hllDeltas = emptyMap(),
        writtenTrie = LiveTable.FinishedBlock.WrittenTrie(
            trieKey = "test-trie",
            dataFileSize = 42,
            trieMetadata = trieMetadata {}
        )
    )

    /**
     * Records what the term appends instead of writing it. Both messages of a block cut reach the replica
     * log through this driver, so this is the whole of what the cycle emits — no log, no tail, and no
     * consume-back to wait on.
     */
    private class RecordingDriver : LogProcessor.LogsDriver {
        val appended = mutableListOf<ReplicaMessage>()

        override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata {
            appended += msg
            return Log.MessageMetadata(0, appended.size - 1L, Instant.now())
        }

        override suspend fun requestFlushBlock(expectedBlockIdx: Long) =
            error("the cutter does not ask for a flush")
    }

    private class Fixture(val cutter: BlockCutter, private val driver: RecordingDriver) {
        val appended get() = driver.appended.toList()
    }

    /**
     * A cutter with the real appender behind it, so a test reads what the cycle wrote rather than asking
     * what it called. The appender writes from its own coroutine, so a queued message lands on the next
     * `runCurrent`.
     */
    private fun TestScope.fixture(
        blockThreshold: Long = IndexerConfig().rowsPerBlock,
        rowsAlreadyApplied: Long = 0,
        finishedBlocks: Map<TableRef, LiveTable.FinishedBlock> = emptyMap(),
    ): Fixture {
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { rowsPerBlock } returns blockThreshold
            every { blockRowCount } returns rowsAlreadyApplied
            every { latestCompletedTx } returns null
            coEvery { finishBlock(any(), any()) } returns finishedBlocks
        }
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }

        val partitionState = PartitionState(TableCatalog(bufferPool), createTrieCatalog(), liveIndex)
        val partitionStorage = PartitionStorage(
            DatabaseLogs(
                InMemoryLog<SourceMessage>(InstantSource.system(), 0),
                InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
            ),
            bufferPool, null
        )

        val driver = RecordingDriver()
        val appender = ReplicaLogAppender(driver)
        backgroundScope.launch { appender.run() }

        return Fixture(
            BlockCutter(
                partitionStorage, partitionState, "test", leaderTerm = 1, replicaAppender = appender,
                logsDriver = driver, compactor = mockk(relaxed = true), dbCatalog = null, meterRegistry = null,
                lastUploadEpochSeconds = AtomicLong(0), scope = backgroundScope,
                ioDispatcher = StandardTestDispatcher(testScheduler)
            ),
            driver
        )
    }

    @Test
    fun `a cut emits the boundary, and the upload behind it the matching BlockUploaded`() = runTest {
        val f = fixture(finishedBlocks = mapOf(tableRef to finishedBlock))
        val token = byteArrayOf(1, 2, 3)

        f.cutter.cut(latestProcessedMsgId = 7, extToken = token)
        runCurrent()

        val boundary = assertInstanceOf(BlockBoundary::class.java, f.appended.single())
        assertEquals(0, boundary.blockIndex, "the first block a database cuts is block 0")
        assertEquals(7, boundary.latestProcessedMsgId)
        assertArrayEquals(token, boundary.externalSourceToken)

        f.cutter.upload(boundaryMsgId = 0, boundary = boundary)
        runCurrent()

        val uploaded = assertInstanceOf(BlockUploaded::class.java, f.appended.last())
        assertEquals(0, uploaded.blockIndex)
        assertEquals(7, uploaded.latestProcessedMsgId)
        assertArrayEquals(token, uploaded.externalSourceToken)
        assertEquals(listOf("test-trie"), uploaded.tries.map { it.trieKey })
    }

    @Test
    fun `nothing resolves between a cut and its upload`() = runTest {
        val f = fixture()
        assertTrue(f.cutter.acceptingResolution)

        f.cutter.cut(latestProcessedMsgId = 0, extToken = null)
        runCurrent()

        assertFalse(f.cutter.acceptingResolution, "resolution is refused for the length of the cut")
        assertThrows<IllegalStateException> { f.cutter.addRows(1) }

        f.cutter.upload(0, f.appended.single() as BlockBoundary)
        assertTrue(f.cutter.acceptingResolution, "the upload re-opens the block behind it")
    }

    @Test
    fun `an empty block is never full, whatever the threshold`() = runTest {
        val f = fixture(blockThreshold = 0)

        assertFalse(f.cutter.isFull, "cutting an empty block would live-lock the term")

        f.cutter.addRows(1)
        assertTrue(f.cutter.isFull)
    }

    @Test
    fun `a block inherited part-filled is full where the previous leader would have cut it`() = runTest {
        val f = fixture(blockThreshold = 10, rowsAlreadyApplied = 9)

        assertFalse(f.cutter.isFull)

        f.cutter.addRows(1)
        assertTrue(f.cutter.isFull, "the gauge is seeded from the rows already applied into the open block")
    }
}
