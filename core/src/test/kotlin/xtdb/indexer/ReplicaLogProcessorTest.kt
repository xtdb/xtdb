package xtdb.indexer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.storage.ObjectStore
import xtdb.block.proto.block
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.time.LocalDate
import java.time.ZoneOffset

class ReplicaLogProcessorTest {

    private fun inst(day: Int) =
        LocalDate.of(2020, 1, day).atStartOfDay().toInstant(ZoneOffset.UTC)

    private fun flusher(prevBlockTxId: Long, flushedTxId: Long) = ReplicaLogProcessor.Flusher(
        Duration.ofDays(2),
        inst(1),
        previousBlockTxId = prevBlockTxId,
        flushedTxId = flushedTxId
    )

    private fun resolvedTx(txId: Long = 0) = Log.Message.ResolvedTx(
        txId = txId,
        systemTimeMicros = Instant.now().toEpochMilli() * 1000,
        committed = true,
        error = ByteArray(0),
        tableData = emptyMap()
    )

    @Test
    fun `test checkBlockTimeout`() {
        flusher(prevBlockTxId = -1, flushedTxId = -1).run {
            assertFalse(
                checkBlockTimeout(inst(2), currentBlockTxId = -1, latestCompletedTxId = 0),
                "checked recently, don't check again"
            )

            assertEquals(inst(1), lastFlushCheck, "don't update lastFlushCheck")
        }


        flusher(prevBlockTxId = 10, flushedTxId = 32).run {
            assertTrue(
                checkBlockTimeout(inst(4), currentBlockTxId = 10, latestCompletedTxId = 40),
                "we've not flushed recently, we have new txs, submit msg"
            )

            assertEquals(inst(4), lastFlushCheck)
        }

        flusher(prevBlockTxId = 10, flushedTxId = 32).run {
            assertFalse(
                checkBlockTimeout(inst(4), currentBlockTxId = 10, latestCompletedTxId = 32),
                "we've not flushed recently, no new txs, don't submit msg"
            )

            assertEquals(inst(1), lastFlushCheck)
        }
    }

    @Test
    fun `buffer overflow stops ingestion`() {
        val log = InMemoryLog(InstantSource.system(), 0)
        val blockCatalog = BlockCatalog("test", null)
        // First ResolvedTx triggers isFull → pendingBlockIdx set.
        // Subsequent records get buffered → overflow on 3rd buffered.
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { isFull() } returns true
        }

        val dbStorage = DatabaseStorage(log, log, BufferPool.UNUSED, null)
        val dbState = DatabaseState(
            "test", blockCatalog,
            mockk<TableCatalog>(relaxed = true), mockk<TrieCatalog>(relaxed = true),
        )

        RootAllocator().use { allocator ->
            val lp = ReplicaLogProcessor(
                allocator, SimpleMeterRegistry(),
                dbStorage, dbState,
                mockk<Indexer.ForDatabase>(relaxed = true),
                liveIndex,
                mockk<Compactor.ForDatabase>(relaxed = true),
                Duration.ofHours(1), emptySet(),
                Database.Mode.READ_ONLY,
                maxBufferedRecords = 2
            )

            val now = Instant.now()
            // ResolvedTx triggers pendingBlockIdx via isFull().
            // Then 3 more records get buffered, exceeding maxBufferedRecords=2.
            val records = listOf(
                Log.Record(0, now, resolvedTx(0)),
                Log.Record(1, now, Log.Message.FlushBlock(999)),
                Log.Record(2, now, Log.Message.FlushBlock(999)),
                Log.Record(3, now, Log.Message.FlushBlock(999)),
            )

            assertThrows<Exception> { lp.processRecords(records) }
            assertNotNull(lp.ingestionError, "ingestion error should be set after buffer overflow")
        }
    }

    @Test
    fun `replay handles block transitions during replay`() {
        val log = InMemoryLog(InstantSource.system(), 0)
        val blockCatalog = BlockCatalog("test", null)
        var fullCount = 0
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            // First call to isFull() (after first ResolvedTx) → true, sets pendingBlockIdx=0.
            // After block 0 transition + replay, second ResolvedTx calls isFull() → true again, sets pendingBlockIdx=1.
            // After block 1 transition, no more ResolvedTx so no more isFull() calls.
            every { isFull() } answers { ++fullCount; true }
        }

        val blocks = mapOf(
            0L to block { blockIndex = 0 }.toByteArray(),
            1L to block { blockIndex = 1 }.toByteArray(),
        )
        val blockFiles = blocks.keys.map { ObjectStore.StoredObject(BlockCatalog.blockFilePath(it), 0) }
        val bufferPool = mockk<BufferPool> {
            every { epoch } returns 0
            every { listAllObjects(any<Path>()) } returns blockFiles
            every { getByteArray(any()) } answers {
                val path = firstArg<Path>()
                blocks.entries.first { BlockCatalog.blockFilePath(it.key) == path }.value
            }
        }

        RootAllocator().use { allocator ->
            val dbStorage = DatabaseStorage(log, log, bufferPool, null)
            val dbState = DatabaseState(
                "test", blockCatalog,
                mockk<TableCatalog>(relaxed = true), mockk<TrieCatalog>(relaxed = true),
            )

            val lp = ReplicaLogProcessor(
                allocator, SimpleMeterRegistry(),
                dbStorage, dbState,
                mockk<Indexer.ForDatabase>(relaxed = true),
                liveIndex,
                mockk<Compactor.ForDatabase>(relaxed = true),
                Duration.ofHours(1), emptySet(),
                Database.Mode.READ_ONLY,
            )

            val now = Instant.now()
            // ResolvedTx(0) triggers isFull → pendingBlockIdx=0, starts buffering.
            // ResolvedTx(1) is buffered.
            // BlockUploaded(1) is buffered.
            // BlockUploaded(0) matches pendingBlockIdx=0 → transition block 0 → replay buffered.
            // During replay: ResolvedTx(1) triggers isFull → pendingBlockIdx=1, starts buffering again.
            // BlockUploaded(1) now matches → transition block 1.
            val records = listOf(
                Log.Record(0, now, resolvedTx(0)),
                Log.Record(1, now, resolvedTx(1)),
                Log.Record(2, now, Log.Message.BlockUploaded(1, 0, 0)),
                Log.Record(3, now, Log.Message.BlockUploaded(0, 0, 0)),
            )

            lp.processRecords(records)

            // Both block transitions should complete.
            assertEquals(1L, blockCatalog.currentBlockIndex)
            assertNull(lp.ingestionError)
        }
    }
}
