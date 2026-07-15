package xtdb.indexer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.*
import kotlinx.coroutines.awaitCancellation
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.block.proto.block
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.api.error.Fault
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import xtdb.util.closeAll
import java.time.Instant

class FollowerLogProcessorTest {

    private lateinit var allocator: RootAllocator
    private lateinit var bufferPool: BufferPool
    private lateinit var liveIndex: LiveIndex
    private lateinit var compactor: Compactor.ForDatabase
    private lateinit var watchers: Watchers
    private lateinit var blockCatalog: BlockCatalog
    private lateinit var tableCatalog: TableCatalog
    private lateinit var trieCatalog: TrieCatalog
    private lateinit var dbState: DatabaseState
    private lateinit var replicaLog: Log<ReplicaMessage>

    // runTest cancels and joins backgroundScope before tearDown, so the followers are quiescent here
    // and freed before `allocator` closes.
    private val followersToClose = mutableListOf<AutoCloseable>()

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        bufferPool = mockk(relaxed = true)
        liveIndex = mockk(relaxed = true)
        compactor = mockk(relaxed = true)
        blockCatalog = BlockCatalog("test", null)
        tableCatalog = mockk(relaxed = true)
        trieCatalog = mockk(relaxed = true)
        dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // The processor self-launches its replica tail; park it so it idles while the test drives
        // processRecords directly — a relaxed mock would return immediately and tear the proc down.
        replicaLog = mockk(relaxed = true)
        coEvery { replicaLog.tailAll(any(), any()) } coAnswers { awaitCancellation() }

        every { bufferPool.epoch } returns 1
    }

    @AfterEach
    fun tearDown() {
        followersToClose.closeAll()
        allocator.close()
    }

    private fun TestScope.makeProcessor(
        maxBufferedRecords: Int = 1024,
        hasExternalSource: Boolean = false,
        meterRegistry: MeterRegistry? = null,
    ) =
        FollowerLogProcessor(
            allocator, replicaLog, bufferPool, dbState, compactor,
            watchers, null, null, afterReplicaMsgId = -1L, backgroundScope,
            hasExternalSource = hasExternalSource,
            meterRegistry = meterRegistry,
            maxBufferedRecords = maxBufferedRecords,
        ).also(followersToClose::add)

    private fun <M> record(offset: Long, message: M) =
        Log.Record(0, offset, Instant.now(), message)

    @Test
    fun `buffer overflow stops ingestion`() = runTest {
        val proc = makeProcessor(maxBufferedRecords = 2)

        val records = listOf(
            record(0, ReplicaMessage.BlockBoundary(0, 0)),
            record(1, ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap())),
            record(2, ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap())),
            record(3, ReplicaMessage.ResolvedTx(3, Instant.now(), true, null, emptyMap())),
        )

        assertThrows<Fault> { proc.processRecords(records) }    }

    @Test
    fun `ResolvedTx skips already-applied transactions`() = runTest {
        watchers = Watchers(latestTxId = 42, latestSourceMsgId = 42)
        val proc = makeProcessor()

        val tx40 = ReplicaMessage.ResolvedTx(40, Instant.now(), true, null, emptyMap(), srcMsgId = 40)
        val tx42 = ReplicaMessage.ResolvedTx(42, Instant.now(), true, null, emptyMap(), srcMsgId = 42)
        val tx43 = ReplicaMessage.ResolvedTx(43, Instant.now(), true, null, emptyMap(), srcMsgId = 43)

        proc.processRecords(listOf(record(0, tx40), record(1, tx42), record(2, tx43)))

        verify(exactly = 0) { liveIndex.commitTx(match { it.txId == tx40.txId }, any()) }
        verify(exactly = 0) { liveIndex.commitTx(match { it.txId == tx42.txId }, any()) }
        verify { liveIndex.commitTx(match { it.txId == tx43.txId }, any()) }
    }

    @Test
    fun `skips stale messages when starting ahead of replica log`() = runTest {
        // simulates MW block with latestProcessedMsgId=1000, no boundaryReplicaMsgId,
        // replaying a replica log that has old SW-era messages.
        // block catalog starts at block 5 (simulating startup from latest block).
        val startBlock = block { blockIndex = 5 }
        blockCatalog = BlockCatalog("test", startBlock)
        dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        watchers = Watchers(latestTxId = 1000, latestSourceMsgId = 1000)
        val proc = makeProcessor()

        val staleRecords = listOf(
            record(0, ReplicaMessage.ResolvedTx(500, Instant.now(), true, null, emptyMap(), srcMsgId = 500)),
            record(1, ReplicaMessage.TriesAdded(1, 1, emptyList(), sourceMsgId = 600)),
            record(2, ReplicaMessage.BlockBoundary(1, 700)),
            record(3, ReplicaMessage.BlockUploaded(1, 1, 1, 800, emptyList())),
            // at the boundary — should also be skipped
            record(4, ReplicaMessage.ResolvedTx(1000, Instant.now(), true, null, emptyMap(), srcMsgId = 1000)),
        )

        proc.processRecords(staleRecords)

        verify(exactly = 0) { liveIndex.commitTx(any(), any()) }
        assert(watchers.latestSourceMsgId == 1000L) { "latestSourceMsgId should not have changed" }
    }

    @Test
    fun `block boundary not skipped when isFull triggers on same txId`() = runTest {
        // Simulates the replica log sequence when isFull() triggers on the leader:
        // the BlockBoundary's latestProcessedMsgId equals the preceding ResolvedTx's txId.
        // The follower must still process the block transition.
        val proc = makeProcessor()

        val blockProto = block { blockIndex = 0 }.toByteArray()
        every { bufferPool.getByteArray(BlockCatalog.blockFilePath(0)) } returns blockProto

        assertNull(blockCatalog.currentBlockIndex, "no block before processing")

        val txId = 100L
        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(txId, Instant.now(), true, null, emptyMap())),
            record(1, ReplicaMessage.BlockBoundary(0, txId)),
            record(2, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, txId, emptyList())),
        ))

        assertEquals(0L, blockCatalog.currentBlockIndex,
            "block catalog should advance to block 0 even when BlockBoundary.latestProcessedMsgId == last txId")
    }

    @Test
    fun `processes messages after skipping stale ones`() = runTest {
        watchers = Watchers(latestTxId = 1000, latestSourceMsgId = 1000)
        val proc = makeProcessor()

        val tx1001 = ReplicaMessage.ResolvedTx(1001, Instant.now(), true, null, emptyMap(), srcMsgId = 1001)

        proc.processRecords(listOf(
            // stale
            record(0, ReplicaMessage.TriesAdded(1, 1, emptyList(), sourceMsgId = 500)),
            // current
            record(1, tx1001),
        ))

        verify { liveIndex.commitTx(match { it.txId == tx1001.txId }, any()) }
        assert(watchers.latestSourceMsgId == 1001L)
    }

    @Test
    fun `ext-source ResolvedTx does not advance latestSourceMsgId`() = runTest {
        // Reproduces #5580: an ext-source ResolvedTx (srcMsgId=null) followed by a BlockBoundary
        // whose latestProcessedMsgId reflects the leader's still-default source watermark would
        // previously violate `srcMsgId >= latestSourceMsgId` on the follower.
        val proc = makeProcessor(hasExternalSource = true)

        val blockProto = block { blockIndex = 0 }.toByteArray()
        every { bufferPool.getByteArray(BlockCatalog.blockFilePath(0)) } returns blockProto

        val extTx = ReplicaMessage.ResolvedTx(0, Instant.now(), true, null, emptyMap(), srcMsgId = null)
        proc.processRecords(listOf(
            record(0, extTx),
            record(1, ReplicaMessage.BlockBoundary(0, -1)),
            record(2, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, -1, emptyList())),
        ))

        verify { liveIndex.commitTx(match { it.txId == extTx.txId }, any()) }
        assertEquals(-1L, watchers.latestSourceMsgId,
            "ext-source ResolvedTx must not bump latestSourceMsgId")
        assertEquals(0L, blockCatalog.currentBlockIndex)
    }

    @Test
    fun `mixed ext-source and source-log ResolvedTxs advance the right watermarks`() = runTest {
        val proc = makeProcessor(hasExternalSource = true)

        val ext0 = ReplicaMessage.ResolvedTx(0, Instant.now(), true, null, emptyMap(), srcMsgId = null)
        val src1 = ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1)
        val ext2 = ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), srcMsgId = null)

        proc.processRecords(listOf(record(0, ext0), record(1, src1), record(2, ext2)))

        verify { liveIndex.commitTx(match { it.txId == ext0.txId }, any()) }
        verify { liveIndex.commitTx(match { it.txId == src1.txId }, any()) }
        verify { liveIndex.commitTx(match { it.txId == ext2.txId }, any()) }
        assertEquals(1L, watchers.latestSourceMsgId,
            "latestSourceMsgId reflects only the source-log tx; ext-source txs leave it alone")
    }

    @Test
    fun `records replica processing metrics`() = runTest {
        val registry = SimpleMeterRegistry()
        val proc = makeProcessor(meterRegistry = registry)

        val blockProto = block { blockIndex = 0 }.toByteArray()
        every { bufferPool.getByteArray(BlockCatalog.blockFilePath(0)) } returns blockProto

        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap())),
            record(1, ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap())),
            record(2, ReplicaMessage.BlockBoundary(0, 2)),
            record(3, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, 2, emptyList())),
        ))

        fun timerCount(msgType: String) = registry.find("xtdb.replica.process.timer")
            .tags("db", "test", "msg.type", msgType)
            .timer()?.count() ?: 0L

        assertEquals(2L, timerCount("ResolvedTx"))
        assertEquals(1L, timerCount("BlockBoundary"))
        assertEquals(1L, timerCount("BlockUploaded"))

        val bufferTimer = registry.find("xtdb.replica.block.buffer.timer").tag("db", "test").timer()
        assertNotNull(bufferTimer, "block buffer timer should be registered")
        assertEquals(1L, bufferTimer!!.count(), "one block buffer window")
        assertTrue(bufferTimer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) >= 0)

        val bufferedRecords = registry.find("xtdb.replica.block.buffered.records").tag("db", "test").summary()
        assertNotNull(bufferedRecords, "buffered records summary should be registered")
        assertEquals(1L, bufferedRecords!!.count())
        assertEquals(0.0, bufferedRecords.totalAmount(), "no records buffered when BlockUploaded follows BlockBoundary directly")
    }

    @Test
    fun `buffered records summary captures size when records arrive between boundary and uploaded`() = runTest {
        val registry = SimpleMeterRegistry()
        val proc = makeProcessor(meterRegistry = registry)

        val blockProto = block { blockIndex = 0 }.toByteArray()
        every { bufferPool.getByteArray(BlockCatalog.blockFilePath(0)) } returns blockProto

        proc.processRecords(listOf(
            record(0, ReplicaMessage.BlockBoundary(0, 0)),
            // these get buffered while we wait for BlockUploaded
            record(1, ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap())),
            record(2, ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap())),
            record(3, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, 0, emptyList())),
        ))

        val bufferedRecords = registry.find("xtdb.replica.block.buffered.records").tag("db", "test").summary()
        assertNotNull(bufferedRecords)
        assertEquals(1L, bufferedRecords!!.count())
        assertEquals(2.0, bufferedRecords.totalAmount(), "two records buffered between boundary and uploaded")
    }
}
