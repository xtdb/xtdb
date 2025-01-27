package xtdb.indexer

import xtdb.api.log.Log
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class LogProcessorTest {

    private fun inst(day: Int) =
        LocalDate.of(2020, 1, day).atStartOfDay().toInstant(ZoneOffset.UTC)

    private fun flusher(prevChunkTxId: Long, flushedTxId: Long) = LogProcessor.Flusher(
        Duration.ofDays(2),
        inst(1),
        previousChunkTxId = prevChunkTxId,
        flushedTxId = flushedTxId
    )

    @Test
    fun `test checkChunkTimeout`() {
        flusher(prevChunkTxId = -1, flushedTxId = -1).run {
            assertNull(
                checkChunkTimeout(inst(2), currentChunkTxId = -1, latestCompletedTxId = 0),
                "checked recently, don't check again"
            )

            assertEquals(inst(1), lastFlushCheck, "don't update lastFlushCheck")
        }


        flusher(prevChunkTxId = 10, flushedTxId = 32).run {
            assertEquals(
                Log.Message.FlushChunk(10),
                checkChunkTimeout(inst(4), currentChunkTxId = 10, latestCompletedTxId = 40),
                "we've not flushed recently, we have new txs, submit msg"
            )

            assertEquals(inst(4), lastFlushCheck)
        }

        flusher(prevChunkTxId = 10, flushedTxId = 32).run {
            assertNull(
                checkChunkTimeout(inst(4), currentChunkTxId = 10, latestCompletedTxId = 32),
                "we've not flushed recently, no new txs, don't submit msg"
            )

            assertEquals(inst(1), lastFlushCheck)
        }
    }
}