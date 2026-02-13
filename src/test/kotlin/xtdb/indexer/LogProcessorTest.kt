package xtdb.indexer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals

class LogProcessorTest {

    private fun inst(day: Int) =
        LocalDate.of(2020, 1, day).atStartOfDay().toInstant(ZoneOffset.UTC)

    private fun flusher(prevBlockTxId: Long, flushedTxId: Long) = LogProcessor.Flusher(
        Duration.ofDays(2),
        inst(1),
        previousBlockTxId = prevBlockTxId,
        flushedTxId = flushedTxId
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
        val liveIndex = mockk<LiveIndex>(relaxed = true) { every { isFull() } returns false }

        val dbStorage = DatabaseStorage(log, log, BufferPool.UNUSED, null)
        val dbState = DatabaseState(
            "test", blockCatalog,
            mockk<TableCatalog>(relaxed = true), mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )

        RootAllocator().use { allocator ->
            val lp = LogProcessor(
                allocator, SimpleMeterRegistry(),
                dbStorage, dbState,
                mockk<Indexer.ForDatabase>(relaxed = true),
                mockk<Compactor.ForDatabase>(relaxed = true),
                Duration.ofHours(1), emptySet(),
                Database.Mode.READ_ONLY,
                maxBufferedRecords = 2
            )

            val now = Instant.now()
            // FlushBlock(-1) matches currentBlockIndex(null → -1), triggers pendingBlockIdx
            // Subsequent FlushBlock(999) don't match → get buffered → overflow on 3rd
            val records = listOf(
                Log.Record(0, now, Log.Message.FlushBlock(-1)),
                Log.Record(1, now, Log.Message.FlushBlock(999)),
                Log.Record(2, now, Log.Message.FlushBlock(999)),
                Log.Record(3, now, Log.Message.FlushBlock(999)),
            )

            assertThrows<Exception> { lp.processRecords(records) }
            assertNotNull(lp.ingestionError, "ingestion error should be set after buffer overflow")
        }
    }
}