package xtdb.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TxIdUtilTest {
    @Test
    fun testRoundTripZeroEpoch() {
        val epoch = 0
        val offset = 123L
        val txId = TxIdUtil.offsetToTxId(epoch, offset)

        // Check that the txId is equal to the offset when epoch is 0
        assertEquals(offset, txId)

        assertEquals(epoch, TxIdUtil.txIdToEpoch(txId))
        assertEquals(offset, TxIdUtil.txIdToOffset(txId))
    }

    @Test
    fun testRoundTripNonZeroEpoch() {
        val epoch = 3
        val offset = 123L
        val txId = TxIdUtil.offsetToTxId(epoch, offset)

        assertEquals(epoch, TxIdUtil.txIdToEpoch(txId))
        assertEquals(offset, TxIdUtil.txIdToOffset(txId))
    }
}
