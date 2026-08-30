package xtdb.indexer

import kotlinx.coroutines.CancellationException
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

/**
 * The replica log's write end for one leader term: everything the term resolves is queued here and appended
 * in that order.
 *
 * Appending is its own coroutine so that the serialization and the log round-trip stay off the term's work
 * loop — which is what releases the source-log tail at "resolved" rather than at "durable" (#5741).
 */
internal class ReplicaLogAppender(
    private val driver: LeaderDriver,
    private val leaderTerm: Long,
    private val electionDriver: ElectionDriver,
) {

    // Unbounded: the term queues here from the same coroutine that services its consume-back, so a bounded
    // channel could block that send — and consume-back is what makes the progress the send would be
    // waiting on. Backpressure comes from the block-cut pause and the term's row gauge.
    private val queue = Channel<AppendItem>(Channel.UNLIMITED)

    suspend fun append(item: AppendItem) = queue.send(item)

    /**
     * Append until the queue is shut down.
     *
     * Plain, non-transactional appends: the sole fence on a zombie leader is the term its records carry,
     * checked when it reads them back — a higher term means it has been superseded, and it resigns (#5817).
     *
     * A biased `select`, so the assertion arms only once the queue has nothing ready — a leader with
     * traffic is asserting itself implicitly, and the timeout restarts on every iteration the queue wins.
     */
    suspend fun run() {
        while (true) {
            val item = select<AppendItem?> {
                queue.onReceiveCatching { res ->
                    res.exceptionOrNull()?.let { throw it }
                    res.getOrNull()
                }

                electionDriver.run { onAssertTimeout { ControlItem(NoOp(termId = leaderTerm)) } }
            } ?: break

            driver.appendToReplica(item.toReplicaMessage())
        }
    }

    /**
     * Stop appending, which is what keeps a resigned term from writing on.
     *
     * A close rather than a cancel, so an append already in flight lands rather than tearing. Whatever is
     * still queued is dropped: its `ResolvedTx` is borrowed, and the resolver frees it.
     */
    fun shutdown(cause: Throwable) = queue.close(cause.asCancellation())
}
