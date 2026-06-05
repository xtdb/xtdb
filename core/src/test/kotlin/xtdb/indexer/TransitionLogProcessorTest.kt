package xtdb.indexer

import io.mockk.*
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.database.DatabaseState
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.Instant

class TransitionLogProcessorTest {

    private lateinit var allocator: RootAllocator
    private lateinit var bufferPool: BufferPool
    private lateinit var liveIndex: LiveIndex
    private lateinit var blockUploader: BlockUploader
    private lateinit var replicaProducer: Log.AtomicProducer<ReplicaMessage>
    private lateinit var watchers: Watchers
    private lateinit var blockCatalog: BlockCatalog
    private lateinit var tableCatalog: TableCatalog
    private lateinit var trieCatalog: TrieCatalog
    private lateinit var dbState: DatabaseState

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        bufferPool = mockk(relaxed = true)
        liveIndex = mockk(relaxed = true)
        blockUploader = mockk(relaxed = true)
        replicaProducer = mockk(relaxed = true)
        blockCatalog = BlockCatalog("test", null)
        tableCatalog = mockk(relaxed = true)
        trieCatalog = mockk(relaxed = true)
        dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        every { bufferPool.epoch } returns 1
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    private fun makeProcessor(hasExternalSource: Boolean = false) =
        TransitionLogProcessor(
            allocator, bufferPool, dbState, liveIndex,
            blockUploader, replicaProducer,
            watchers, null, afterReplicaMsgId = -1L,
            hasExternalSource = hasExternalSource,
        )

    private fun <M> record(offset: Long, message: M) =
        Log.Record(0, offset, Instant.now(), message)

    @Test
    fun `block boundary not skipped when isFull triggers on same txId`() = runTest {
        val proc = makeProcessor()

        val txId = 100L
        proc.processRecords(listOf(
            record(0, ReplicaMessage.ResolvedTx(txId, Instant.now(), true, null, emptyMap(), srcMsgId = txId)),
            record(1, ReplicaMessage.BlockBoundary(0, txId)),
        ))

        coVerify { blockUploader.uploadBlock(replicaProducer, 1, any()) }

        proc.close()
    }

    @Test
    fun `ext-source ResolvedTx does not advance latestSourceMsgId`() = runTest {
        // Companion to the FollowerLogProcessorTest case: the transition path must also keep
        // ext-source txs from bumping latestSourceMsgId, so the subsequent BlockBoundary's
        // latestProcessedMsgId (= leader's still-default -1) doesn't violate the Watchers invariant.
        val proc = makeProcessor(hasExternalSource = true)

        val extTx = ReplicaMessage.ResolvedTx(0, Instant.now(), true, null, emptyMap(), srcMsgId = null)
        proc.processRecords(listOf(
            record(0, extTx),
            record(1, ReplicaMessage.BlockBoundary(0, -1)),
        ))

        verify { liveIndex.importTx(extTx) }
        assertEquals(-1L, watchers.latestSourceMsgId,
            "ext-source ResolvedTx must not bump latestSourceMsgId")

        proc.close()
    }
}
