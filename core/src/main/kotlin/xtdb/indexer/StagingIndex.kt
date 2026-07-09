package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.database.ExternalSourceToken
import xtdb.util.closeAll

/**
 * The leader's staging area for resolved-but-not-yet-durable transactions. Standing state the
 * [LeaderLogProcessor] owns from construction and frees in its two-phase close (after the resolver
 * job is joined); the resolver is its sole accessor, so it needs no lock.
 *
 * As each tx resolves, the resolver holds its owned row slices ([StagedTx]) plus the replica
 * [message][ReplicaMessage.ResolvedTx] to be appended at seal in the `accumulating` slot, and advances
 * the [applied head][latestCompletedTx]. Nothing is appended to a producer transaction here — the append
 * happens when the resolver seals the batch (a single fenced producer permits only one open transaction
 * at a time, so appends are queued into one transaction and committed together).
 *
 * Two heads: this [latestCompletedTx] is the APPLIED head (drives resolution — next external-source
 * tx-id and system-time smoothing), which leads [LiveIndex.latestCompletedTx] (the durable/query basis,
 * advanced by promotion) by the txs staged but not yet promoted.
 */
class StagingIndex(
    allocator: BufferAllocator,
    latestCompletedTx: TransactionKey?,
) : AutoCloseable {

    private val allocator = allocator.newChildAllocator("staging-index", 0, Long.MAX_VALUE)

    // Resolver-confined (its sole accessor), so a plain var — no cross-thread sharing to guard.
    var latestCompletedTx: TransactionKey? = latestCompletedTx
        private set

    /** A resolved tx held for staging: its owned row slices plus the replica message to append at seal. */
    class Pending(
        val stagedTx: StagedTx,
        val message: ReplicaMessage.ResolvedTx,
    ) : AutoCloseable {
        override fun close() = stagedTx.close()
    }

    private val accumulating = ArrayDeque<Pending>()

    /** In-flight staged predecessors for resolution layering (read-your-writes), oldest→newest. */
    val stagedTxs: List<StagedTx> get() = accumulating.map { it.stagedTx }

    val isEmpty: Boolean get() = accumulating.isEmpty()

    /**
     * Stage a resolved [openTx] (whose replica [message] the resolver holds for seal-time append): take
     * independent slices of its writes into the staging allocator, hold them + the message in the
     * accumulating slot, and advance the applied head. The caller closes [openTx] afterwards.
     */
    fun stage(
        openTx: OpenTx, message: ReplicaMessage.ResolvedTx,
        notifyMsgId: MessageId, txResult: TransactionResult, externalSourceToken: ExternalSourceToken?,
    ) {
        // StagedTx.stage cleans up its own partial slices on throw; nothing else here can leak.
        val stagedTx = StagedTx.stage(allocator, openTx, notifyMsgId, txResult, externalSourceToken)
        accumulating.addLast(Pending(stagedTx, message))
        latestCompletedTx = openTx.txKey
    }

    /** Take the accumulated slot as an ordered batch (send order) and clear the slot. */
    fun drainAccumulated(): List<Pending> {
        val batch = accumulating.toList()
        accumulating.clear()
        return batch
    }

    override fun close() {
        accumulating.closeAll()
        allocator.close()
    }
}
