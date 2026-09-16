package xtdb.indexer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import xtdb.TestPartition
import xtdb.api.log.LeaderTerm
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.block.proto.block
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.api.error.Fault
import xtdb.storage.MemoryStorage
import xtdb.util.closeAll
import java.nio.ByteBuffer
import java.time.Instant

private fun FollowerLogProcessor.processRecords(records: List<Log.Record<ReplicaMessage>>) =
    records.forEach { handleRecord(it) }

// The term most of these records carry. The fence starts at 0, so it admits them all, and only the
// tests that name a second term are saying anything about fencing.
private val TERM = LeaderTerm.of(0, 1)

class FollowerLogProcessorTest {

    private val dbName = "test"

    private lateinit var allocator: RootAllocator
    private lateinit var bufferPool: MemoryStorage
    private lateinit var compactor: Compactor.ForDatabase
    private lateinit var watchers: Watchers
    private lateinit var partition: TestPartition

    private val tableCatalog get() = partition.tableCatalog
    private val liveIndex get() = partition.liveIndex

    // runTest cancels and joins backgroundScope before tearDown, so the followers are quiescent here
    // and freed before `allocator` closes.
    private val followersToClose = mutableListOf<AutoCloseable>()

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        // Epoch 1, so a record carrying epoch 0 is one this storage has moved past.
        bufferPool = MemoryStorage(allocator, epoch = 1)
        compactor = mockk(relaxed = true)
        partition = TestPartition(allocator, bufferPool)
        watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
    }

    @AfterEach
    fun tearDown() {
        followersToClose.closeAll()
        partition.close()
        bufferPool.close()
        allocator.close()
    }

    /** Start the table catalog on an already-flushed block, as a node restarting from storage does. */
    private fun restartFromBlock(blockIndex: Long) {
        partition.close()
        partition = TestPartition(allocator, bufferPool, block = block { this.blockIndex = blockIndex })
    }

    /** Put the block file a [ReplicaMessage.BlockUploaded] for [blockIndex] will send the follower to read. */
    private fun writeBlockFile(blockIndex: Long) =
        bufferPool.putObjectSync(
            TableCatalog.blockFilePath(blockIndex),
            ByteBuffer.wrap(block { this.blockIndex = blockIndex }.toByteArray())
        )

    private fun makeProcessor(
        maxBufferedRecords: Int = 1024,
        hasExternalSource: Boolean = false,
        meterRegistry: MeterRegistry? = null,
        pendingBlock: PendingBlock? = null,
    ) =
        FollowerLogProcessor(
            allocator, bufferPool, partition.state, dbName, compactor,
            watchers, null, pendingBlock,
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
            record(0, ReplicaMessage.BlockBoundary(0, 0, termId = TERM)),
            record(1, ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), termId = TERM)),
            record(2, ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), termId = TERM)),
            record(3, ReplicaMessage.ResolvedTx(3, Instant.now(), true, null, emptyMap(), termId = TERM)),
        )

        assertThrows<Fault> { proc.processRecords(records) }    }

    @Test
    fun `a block inherited from the role before it closes on that role's own upload`() = runTest {
        writeBlockFile(0)

        val inherited = PendingBlock(0, ReplicaMessage.BlockBoundary(0, 0, termId = TERM))
        inherited += record(
            1, ReplicaMessage.ResolvedTx(3, Instant.now(), true, null, emptyMap(), srcMsgId = 2, termId = TERM)
        )

        val proc = makeProcessor(pendingBlock = inherited)

        proc.handleRecord(
            record(2, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, 0, emptyList(), termId = TERM))
        )

        assertNull(proc.pendingBlock)
        assertEquals(0L, tableCatalog.currentBlockIndex, "the inherited block closed on the upload it was waiting for")
        assertEquals(3L, watchers.latestTxId, "and the record it was holding applied behind it")
    }

    @Test
    fun `a tx at or below the watermark is skipped, and the one above it applies`() = runTest {
        watchers = Watchers(latestTxId = 42, latestSourceMsgId = 42)
        val proc = makeProcessor()

        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(40, Instant.now(), true, null, emptyMap(), srcMsgId = 40, termId = TERM)),
            record(1, ReplicaMessage.ResolvedTx(42, Instant.now(), true, null, emptyMap(), srcMsgId = 42, termId = TERM)),
            record(2, ReplicaMessage.ResolvedTx(43, Instant.now(), true, null, emptyMap(), srcMsgId = 43, termId = TERM)),
        ))

        // Applying either of the first two would go backwards through `Watchers.notifyApplied`, whose
        // monotonicity check throws — so reaching here is half of what says they were skipped.
        assertEquals(43L, watchers.latestTxId)
        assertEquals(43L, liveIndex.latestCompletedTx?.txId)
    }

    @Test
    fun `a node starting ahead of the replica log skips everything below where it starts`() = runTest {
        // A node whose latest block records a source position of 1000, replaying a log that still holds
        // the records below it.
        restartFromBlock(5)
        watchers = Watchers(latestTxId = 1000, latestSourceMsgId = 1000)
        val proc = makeProcessor()

        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(500, Instant.now(), true, null, emptyMap(), srcMsgId = 500, termId = TERM)),
            record(1, ReplicaMessage.TriesAdded(1, 1, emptyList(), sourceMsgId = 600, termId = TERM)),
            record(2, ReplicaMessage.BlockBoundary(1, 700, termId = TERM)),
            record(3, ReplicaMessage.BlockUploaded(1, 1, 1, 800, emptyList(), termId = TERM)),
            // at the boundary — skipped too
            record(4, ReplicaMessage.ResolvedTx(1000, Instant.now(), true, null, emptyMap(), srcMsgId = 1000, termId = TERM)),
        ))

        assertNull(liveIndex.latestCompletedTx, "nothing applied")
        assertEquals(5L, tableCatalog.currentBlockIndex, "and the catalog did not go back to b1")
        assertEquals(1000L, watchers.latestSourceMsgId)
    }

    @Test
    fun `block boundary not skipped when isFull triggers on same txId`() = runTest {
        // The row gauge cuts the block, so the boundary's latestProcessedMsgId equals the preceding
        // ResolvedTx's txId, with no FlushBlock in between to move it on.
        val proc = makeProcessor()

        writeBlockFile(0)

        assertNull(tableCatalog.currentBlockIndex, "no block before processing")

        val txId = 100L
        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(txId, Instant.now(), true, null, emptyMap(), termId = TERM)),
            record(1, ReplicaMessage.BlockBoundary(0, txId, termId = TERM)),
            record(2, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, txId, emptyList(), termId = TERM)),
        ))

        assertEquals(0L, tableCatalog.currentBlockIndex,
            "block catalog should advance to block 0 even when BlockBoundary.latestProcessedMsgId == last txId")
    }

    @Test
    fun `a stale record does not stop the fresh one behind it`() = runTest {
        watchers = Watchers(latestTxId = 1000, latestSourceMsgId = 1000)
        val proc = makeProcessor()

        proc.processRecords(listOf(
            record(0, ReplicaMessage.TriesAdded(1, 1, emptyList(), sourceMsgId = 500, termId = TERM)),
            record(1, ReplicaMessage.ResolvedTx(1001, Instant.now(), true, null, emptyMap(), srcMsgId = 1001, termId = TERM)),
        ))

        assertEquals(1001L, liveIndex.latestCompletedTx?.txId)
        assertEquals(1001L, watchers.latestSourceMsgId)
    }

    @Test
    fun `ext-source ResolvedTx does not advance latestSourceMsgId`() = runTest {
        // Reproduces #5580: an ext-source ResolvedTx (srcMsgId=null) followed by a BlockBoundary
        // whose latestProcessedMsgId reflects the leader's still-default source watermark would
        // previously violate `srcMsgId >= latestSourceMsgId` on the follower.
        val proc = makeProcessor(hasExternalSource = true)

        writeBlockFile(0)

        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(0, Instant.now(), true, null, emptyMap(), srcMsgId = null, termId = TERM)),
            record(1, ReplicaMessage.BlockBoundary(0, -1, termId = TERM)),
            record(2, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, -1, emptyList(), termId = TERM)),
        ))

        assertEquals(0L, liveIndex.latestCompletedTx?.txId, "the ext tx applied")
        assertEquals(-1L, watchers.latestSourceMsgId,
            "ext-source ResolvedTx must not bump latestSourceMsgId")
        assertEquals(0L, tableCatalog.currentBlockIndex)
    }

    @Test
    fun `mixed ext-source and source-log ResolvedTxs advance the right watermarks`() = runTest {
        val proc = makeProcessor(hasExternalSource = true)

        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(0, Instant.now(), true, null, emptyMap(), srcMsgId = null, termId = TERM)),
            record(1, ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), srcMsgId = 1, termId = TERM)),
            record(2, ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), srcMsgId = null, termId = TERM)),
        ))

        assertEquals(2L, watchers.latestTxId, "all three applied")
        assertEquals(1L, watchers.latestSourceMsgId,
            "latestSourceMsgId reflects only the source-log tx; ext-source txs leave it alone")
    }

    @Test
    fun `records replica processing metrics`() = runTest {
        val registry = SimpleMeterRegistry()
        val proc = makeProcessor(meterRegistry = registry)

        writeBlockFile(0)

        proc.processRecords(listOf(
            record(0, ReplicaMessage.BlockBoundary(0, 0, termId = TERM)),
            record(1, ReplicaMessage.ResolvedTx(1, Instant.now(), true, null, emptyMap(), termId = TERM)),
            record(2, ReplicaMessage.ResolvedTx(2, Instant.now(), true, null, emptyMap(), termId = TERM)),
            record(3, ReplicaMessage.BlockUploaded(Storage.VERSION, 1, 0, 0, emptyList(), termId = TERM)),
        ))

        fun timerCount(msgType: String) = registry.find("xtdb.replica.process.timer")
            .tags("db", "test", "msg.type", msgType)
            .timer()?.count() ?: 0L

        assertEquals(2L, timerCount("ResolvedTx"))
        assertEquals(1L, timerCount("BlockBoundary"))
        assertEquals(1L, timerCount("BlockUploaded"))

        val bufferTimer = registry.find("xtdb.replica.block.buffer.timer").tag("db", "test").timer()
        assertEquals(1L, bufferTimer?.count(), "one block buffer window")

        val bufferedRecords = registry.find("xtdb.replica.block.buffered.records").tag("db", "test").summary()
        assertEquals(1L, bufferedRecords?.count())
        assertEquals(2.0, bufferedRecords?.totalAmount(), "the two records held between boundary and upload")
    }
}
