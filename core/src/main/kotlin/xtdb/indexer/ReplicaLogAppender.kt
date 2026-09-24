package xtdb.indexer

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.NoOp

/**
 * One item queued for append to the replica log.
 *
 * The term is stamped where the item is made, because it is the *resolving* term's claim over the record
 * rather than a property of whoever drains the queue. Its position within the term is the opposite: that
 * is the order the log records, so it is stamped as it is appended. Serialization is what stays deferred:
 * [TxItem] renders its relations to Arrow IPC when appended, which is why the queue carries items and not
 * messages.
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
    private val pipelined: Boolean,
) {

    // Unbounded: the term queues here from the same coroutine that services its consume-back, so a bounded
    // channel could block that send — and consume-back is what makes the progress the send would be
    // waiting on. Backpressure comes from the block-cut pause and the term's row gauge.
    private val queue = Channel<AppendItem>(Channel.UNLIMITED)

    // Held across the enqueue, not just the increment: two writers each holding a position would otherwise race to the log and land out of order.
    private val appendLock = Mutex()

    // Position 0 is the leadership claim that opened this term.
    private var nextTermSeq = 1L

    suspend fun append(item: AppendItem) = queue.send(item)

    private suspend fun enqueueNow(message: ReplicaMessage): Deferred<Log.MessageMetadata> =
        appendLock.withLock { logsDriver.enqueueToReplica(message.withTermSeq(nextTermSeq++)) }

    /** Appends [message] at the term's next position without queueing, returning once the log has it. */
    suspend fun appendNow(message: ReplicaMessage): Log.MessageMetadata = enqueueNow(message).await()

    suspend fun run() {
        try {
            while (true) {
                val item = select {
                    queue.onReceive { it }

                    electionDriver.run { onAssertTimeout { ControlItem(NoOp(termId = leaderTerm)) } }
                }

                val durable = enqueueNow(item.toReplicaMessage())

                // Pipelined, a lost record reaches the term as the gap its read-back voids (#6105), not through this handle.
                if (!pipelined) durable.await()
            }
        } finally {
            queue.cancel()
        }
    }
}
