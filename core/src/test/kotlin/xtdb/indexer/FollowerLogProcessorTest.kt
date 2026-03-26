package xtdb.indexer

import io.mockk.*
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.error.Fault
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
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
        watchers = Watchers(-1)

        every { bufferPool.epoch } returns 1
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    private fun makeProcessor(afterSourceMsgId: Long = -1L, maxBufferedRecords: Int = 1024) =
        FollowerLogProcessor(
            allocator, bufferPool, dbState,
            compactor, watchers, null, null, afterSourceMsgId = afterSourceMsgId, afterReplicaMsgId = -1L, maxBufferedRecords
        )

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

        assertThrows<Fault> { proc.processRecords(records) }
        proc.close()
    }

    @Test
    fun `ResolvedTx skips already-applied transactions`() = runTest {
        watchers = Watchers(42)
        val proc = makeProcessor(afterSourceMsgId = 42)

        val tx40 = ReplicaMessage.ResolvedTx(40, Instant.now(), true, null, emptyMap())
        val tx42 = ReplicaMessage.ResolvedTx(42, Instant.now(), true, null, emptyMap())
        val tx43 = ReplicaMessage.ResolvedTx(43, Instant.now(), true, null, emptyMap())

        proc.processRecords(listOf(record(0, tx40), record(1, tx42), record(2, tx43)))

        verify(exactly = 0) { liveIndex.importTx(tx40) }
        verify(exactly = 0) { liveIndex.importTx(tx42) }
        verify { liveIndex.importTx(tx43) }

        proc.close()
    }

    @Test
    fun `skips stale messages when starting ahead of replica log`() = runTest {
        // simulates MW block with latestProcessedMsgId=1000, no boundaryReplicaMsgId,
        // replaying a replica log that has old SW-era messages
        watchers = Watchers(1000)
        val proc = makeProcessor(afterSourceMsgId = 1000)

        val staleRecords = listOf(
            record(0, ReplicaMessage.ResolvedTx(500, Instant.now(), true, null, emptyMap())),
            record(1, ReplicaMessage.TriesAdded(1, 1, emptyList(), sourceMsgId = 600)),
            record(2, ReplicaMessage.BlockBoundary(1, 700)),
            record(3, ReplicaMessage.BlockUploaded(1, 1, 1, 800, emptyList())),
            // at the boundary — should also be skipped
            record(4, ReplicaMessage.ResolvedTx(1000, Instant.now(), true, null, emptyMap())),
        )

        proc.processRecords(staleRecords)

        verify(exactly = 0) { liveIndex.importTx(any()) }
        assert(proc.latestSourceMsgId == 1000L) { "latestSourceMsgId should not have changed" }

        proc.close()
    }

    @Test
    fun `processes messages after skipping stale ones`() = runTest {
        watchers = Watchers(1000)
        val proc = makeProcessor(afterSourceMsgId = 1000)

        val tx1001 = ReplicaMessage.ResolvedTx(1001, Instant.now(), true, null, emptyMap())

        proc.processRecords(listOf(
            // stale
            record(0, ReplicaMessage.TriesAdded(1, 1, emptyList(), sourceMsgId = 500)),
            // current
            record(1, tx1001),
        ))

        verify { liveIndex.importTx(tx1001) }
        assert(proc.latestSourceMsgId == 1001L)

        proc.close()
    }
}
