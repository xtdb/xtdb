package xtdb.indexer

import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.error.Fault
import xtdb.util.StringUtil.asLexHex

class PendingBlock(
    val boundaryMsgId: MessageId, val boundaryMessage: ReplicaMessage.BlockBoundary,
    private val maxBufferedRecords: Int = 1024
) {
    val blockIdx get() = boundaryMessage.blockIndex
    private val _bufferedRecords = mutableListOf<Log.Record<ReplicaMessage>>()
    val bufferedRecords: List<Log.Record<ReplicaMessage>> get() = _bufferedRecords

    operator fun plusAssign(record: Log.Record<ReplicaMessage>) {
        if (_bufferedRecords.size >= maxBufferedRecords) {
            throw Fault(
                "Follower buffer overflow: buffered $maxBufferedRecords records while waiting for BlockUploaded(b${blockIdx.asLexHex})",
                "xtdb.indexer/follower-buffer-overflow",
                mapOf("pending-block-idx" to blockIdx, "max-buffered-records" to maxBufferedRecords)
            )
        }
        _bufferedRecords += record
    }
}