package xtdb.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MsgIdUtilTest {
    @Test
    fun testRoundTripZeroEpoch() {
        val epoch = 0
        val offset = 123L
        val txId = MsgIdUtil.offsetToMsgId(epoch, offset)

        // Check that the txId is equal to the offset when epoch is 0
        assertEquals(offset, txId)

        assertEquals(epoch, MsgIdUtil.msgIdToEpoch(txId))
        assertEquals(offset, MsgIdUtil.msgIdToOffset(txId))
    }

    @Test
    fun testRoundTripNonZeroEpoch() {
        val epoch = 3
        val offset = 123L
        val txId = MsgIdUtil.offsetToMsgId(epoch, offset)

        assertEquals(epoch, MsgIdUtil.msgIdToEpoch(txId))
        assertEquals(offset, MsgIdUtil.msgIdToOffset(txId))
    }
}
