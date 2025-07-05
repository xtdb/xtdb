package xtdb.util

import xtdb.api.log.LogOffset
import xtdb.api.log.MessageId

object MsgIdUtil {
    private const val EPOCH_LIMIT = 1L shl 16
    private const val OFFSET_LIMIT = 1L shl 48
    private const val OFFSET_MASK = OFFSET_LIMIT - 1

    @JvmStatic
    fun offsetToMsgId(epoch: Int, offset: LogOffset): MessageId {
        require(epoch < EPOCH_LIMIT) { "Epoch value ($epoch) exceeds the limit ($EPOCH_LIMIT)" }
        require(offset < OFFSET_LIMIT) { "Offset value ($offset) exceeds the limit ($OFFSET_LIMIT)" }
        return (epoch.toLong() shl 48) + offset
    }

    @JvmStatic
    fun msgIdToEpoch(msgId: Long): Int = (msgId shr 48).toInt()

    @JvmStatic
    fun msgIdToOffset(msgId: Long): MessageId = msgId and OFFSET_MASK
}
