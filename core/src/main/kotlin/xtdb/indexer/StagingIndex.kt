package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.DbOp
import xtdb.api.log.MessageId
import xtdb.util.closeAll

/**
 * The leader's staging area for resolved-but-not-yet-durable transactions. Standing state the
 * [LeaderLogProcessor] owns from construction and frees in its two-phase close (after the resolver
 * job is joined); the resolver is its sole accessor, so it needs no lock.
 *
 * As each tx resolves, the resolver holds it ([ResolvedTx], owning its row slices) in the `accumulating`
 * slot and advances the [applied head][latestCompletedTx]. Nothing is appended to a producer transaction
 * here — the append happens when the resolver seals the batch (a single fenced producer permits only
 * one open transaction at a time, so appends are queued into one transaction and committed together),
 * where each resolved tx serializes itself into a replica message via [ResolvedTx.toReplicaMessage].
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

    private val accumulating = ArrayDeque<ResolvedTx>()

    /** In-flight staged predecessors for resolution layering (read-your-writes), oldest→newest. */
    val resolvedTxs: List<ResolvedTx> get() = accumulating.toList()

    val isEmpty: Boolean get() = accumulating.isEmpty()

    /**
     * Stage a resolved [openTx]: take independent slices of its writes into the staging allocator, hold
     * them in the accumulating slot, and advance the applied head. The caller closes [openTx] afterwards.
     */
    fun stage(openTx: OpenTx, srcMsgId: MessageId, txResult: TransactionResult, dbOp: DbOp?) {
        // ResolvedTx.stage cleans up its own partial slices on throw; nothing else here can leak.
        accumulating.addLast(ResolvedTx.stage(allocator, openTx, srcMsgId, txResult, dbOp))
        latestCompletedTx = openTx.txKey
    }

    /** Take the accumulated slot as an ordered batch (send order) and clear the slot. */
    fun drainAccumulated(): List<ResolvedTx> {
        val batch = accumulating.toList()
        accumulating.clear()
        return batch
    }

    override fun close() {
        accumulating.closeAll()
        allocator.close()
    }
}
