package xtdb.indexer

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.NoOp

/**
 * One item queued for append to the replica log.
 *
 * The term is stamped where the item is made, because it is the *resolving* term's claim over the record
 * rather than a property of whoever drains the queue. Serialization is what stays deferred: [TxItem]
 * renders its relations to Arrow IPC when appended, which is why the queue carries items and not messages.
 */
internal sealed interface AppendItem {
    fun toReplicaMessage(): ReplicaMessage
}

/** A resolved tx, borrowed — the resolver owns its relations and frees them. */
internal class TxItem(private val resolvedTx: ResolvedTx, private val termId: Long) : AppendItem {
    override fun toReplicaMessage() = resolvedTx.toReplicaMessage(termId)
}

internal class ControlItem(private val message: ReplicaMessage) : AppendItem {
    override fun toReplicaMessage() = message
}

internal class ReplicaLogAppender(
    private val logsDriver: LogProcessor.LogsDriver,
    private val leaderTerm: Long,
    private val electionDriver: ElectionDriver,
) {

    // Unbounded: the term queues here from the same coroutine that services its consume-back, so a bounded
    // channel could block that send — and consume-back is what makes the progress the send would be
    // waiting on. Backpressure comes from the block-cut pause and the term's row gauge.
    private val queue = Channel<AppendItem>(Channel.UNLIMITED)

    suspend fun append(item: AppendItem) = queue.send(item)

    suspend fun run() {
        try {
            while (true) {
                val item = select {
                    queue.onReceive { it }

                    electionDriver.run { onAssertTimeout { ControlItem(NoOp(termId = leaderTerm)) } }
                }

                logsDriver.appendToReplica(item.toReplicaMessage())
            }
        } finally {
            queue.cancel()
        }
    }
}
