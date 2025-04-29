package xtdb.util

object TxIdUtil {
    private const val EPOCH_LIMIT = 1L shl 16
    private const val OFFSET_LIMIT = 1L shl 48
    private const val OFFSET_MASK = OFFSET_LIMIT - 1

    @JvmStatic
    fun offsetToTxId(epoch: Int, offset: Long): Long {
        require(epoch < EPOCH_LIMIT) { "Epoch value ($epoch) exceeds the limit ($EPOCH_LIMIT)" }
        require(offset < OFFSET_LIMIT) { "Offset value ($offset) exceeds the limit ($OFFSET_LIMIT)" }
        return (epoch.toLong() shl 48) + offset
    }

    @JvmStatic
    fun txIdToEpoch(txId: Long): Int = (txId shr 48).toInt()

    @JvmStatic
    fun txIdToOffset(txId: Long): Long = txId and OFFSET_MASK
}
