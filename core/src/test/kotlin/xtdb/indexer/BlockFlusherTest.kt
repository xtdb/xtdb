package xtdb.indexer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset

class BlockFlusherTest {

    private fun inst(day: Int) =
        LocalDate.of(2020, 1, day).atStartOfDay().toInstant(ZoneOffset.UTC)

    private fun flusher(prevBlockTxId: Long) = BlockFlusher(
        Duration.ofDays(2),
        inst(1),
        previousBlockTxId = prevBlockTxId
    )

    @Test
    fun `does not check again within the timeout`() {
        flusher(prevBlockTxId = -1).run {
            assertFalse(checkBlockTimeout(inst(2), currentBlockTxId = -1))
            assertEquals(inst(1), lastFlushCheck, "lastFlushCheck untouched")
        }
    }

    @Test
    fun `cuts a block once the timeout has elapsed`() {
        flusher(prevBlockTxId = 10).run {
            assertTrue(checkBlockTimeout(inst(4), currentBlockTxId = 10))
            assertEquals(inst(4), lastFlushCheck)
        }
    }

    @Test
    fun `re-arms rather than cutting straight after a block landed by another route`() {
        flusher(prevBlockTxId = 10).run {
            assertFalse(
                checkBlockTimeout(inst(4), currentBlockTxId = 32),
                "a block landed since the last check — don't immediately cut another"
            )
            assertEquals(inst(4), lastFlushCheck)
            assertEquals(32, previousBlockTxId)

            assertTrue(
                checkBlockTimeout(inst(7), currentBlockTxId = 32),
                "and cut on the timeout after that"
            )
        }
    }

    @Test
    fun `keeps cutting blocks on a database that never advances`() {
        // The keep-alive #5778 depends on: nothing is being written, so the block catalog's
        // latest-completed-tx never moves, and the timeout must fire every interval regardless.
        flusher(prevBlockTxId = 10).run {
            assertTrue(checkBlockTimeout(inst(4), currentBlockTxId = 10))
            assertTrue(checkBlockTimeout(inst(7), currentBlockTxId = 10))
            assertTrue(checkBlockTimeout(inst(10), currentBlockTxId = 10))
        }
    }
}
