package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import java.time.Duration
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
}