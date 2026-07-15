package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.log.DbOp
import xtdb.types.MessageId
import xtdb.util.closeAll
import xtdb.api.tx.OpenTx

/**
 * The leader's staging area for resolved-but-not-yet-durable transactions. Standing state the
 * [LeaderLogProcessor] owns from construction and frees in its two-phase close (after the resolver
 * job is joined); the resolver is its sole accessor, so it needs no lock.
 *
 * As each tx resolves, the resolver holds it ([ResolvedTx], owning the tx's table relations) in the
 * `accumulating` slot and advances the [applied head][latestCompletedTx]. Nothing is appended to a producer transaction
 * here — the resolver [seal]s the accumulated txs into a batch (a single fenced producer permits only
 * one open transaction at a time, so appends are queued into one transaction and committed together)
 * and from then on owns them itself: the batch rides the leader's in-flight slot until its background
 * append settles and it is promoted into the durable index.
 *
 * Two heads: this [latestCompletedTx] is the APPLIED head (drives resolution — next external-source
 * tx-id and system-time smoothing), which leads [LiveIndex.latestCompletedTx] (the durable/query basis,
 * advanced by promotion) by the txs staged but not yet promoted.
 */
internal class StagingIndex(
    latestCompletedTx: TransactionKey?,
) : AutoCloseable {

    // Resolver-confined (its sole accessor), so a plain var — no cross-thread sharing to guard.
    var latestCompletedTx: TransactionKey? = latestCompletedTx
        private set

    private val accumulating = ArrayDeque<ResolvedTx>()

    /** Staged predecessors for resolution layering (read-your-writes), oldest→newest. */
    val resolvedTxs: List<ResolvedTx> get() = accumulating.toList()

    /** Rows across the accumulating slot's staged txs — derived, so it can't drift. */
    val rowCount: Long get() = accumulating.sumOf { tx -> tx.allTables.sumOf { it.relation.rowCount.toLong() } }

    val isEmpty: Boolean get() = accumulating.isEmpty()

    /**
     * Stage a resolved [openTx]: take ownership of its written tables (a reference move — see
     * `OpenTx.sealTables`), hold them in the accumulating slot, and advance the applied head.
     * The caller closes the (now table-less) [openTx] afterwards.
     */
    fun stage(openTx: OpenTx, srcMsgId: MessageId, txResult: TransactionResult, dbOp: DbOp?, pending: CompletableDeferred<TransactionResult>?) {
        accumulating.addLast(ResolvedTx.stage(openTx, srcMsgId, txResult, dbOp, pending))
        latestCompletedTx = openTx.txKey
    }

    /**
     * Fail every pending deferred in the accumulating slot. Called on teardown paths where staged txs
     * will never settle — the persister is exiting and nobody else will complete them.
     */
    fun failPending(cause: Throwable) = accumulating.forEach { it.pending?.completeExceptionally(cause) }

    /**
     * Take the accumulated txs as an ordered batch (send order), or null if nothing is staged, clearing
     * the slot — ownership of the txs (and freeing them) passes to the caller.
     */
    fun seal(): List<ResolvedTx>? =
        accumulating.toList().takeIf { it.isNotEmpty() }.also { this.accumulating.clear() }

    override fun close() = accumulating.closeAll()
}
