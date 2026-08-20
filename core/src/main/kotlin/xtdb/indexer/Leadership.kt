package xtdb.indexer

import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage

/**
 * What leading adds to the replica-log apply path, and the whole of what it adds.
 *
 * Two hooks, because there are exactly two things a node that produced a record knows which a node
 * merely reading it does not: it is still holding what the record materialises, and it is the node whose
 * local state that record's block boundary cuts. Everything else on the apply path — the fence, the
 * consume position, the source watermark, holding records and releasing them — is the same either way,
 * and [ReplicaApplier] does it without asking.
 *
 * A hook rather than a branch on the role: the applier asks a question of something that may be absent,
 * and the answer when it is absent is the ordinary one. So a new kind of replica message needs no
 * decision here unless it wants a hook of its own, and wanting one is then visible.
 *
 * See `allium/log-processor-lifecycle.allium` (`LeadershipReachesApplyingThroughTwoHooks`).
 */
internal interface Leadership {

    /** The term everything this leadership appends is stamped with. */
    val term: Long

    /**
     * Commit a record this leadership produced, from what it still holds — or `false` if it did not
     * produce it.
     *
     * `true` means done, watermarks advanced: leadership resolved the transaction, attached or detached
     * the database, added the tries it wrote, removed the tries it collected and rolled the index in
     * order to produce the record at all, so the record's own contents are not applied a second time.
     *
     * Safe only because a record and what leadership holds are the same thing by construction — it
     * appends in resolution order and reads back in position order, and those are the same order. A hit
     * whose contents disagree with the record is a fault rather than a case to handle.
     */
    suspend fun applyAuthored(record: Log.Record<ReplicaMessage>): Boolean

    /**
     * Write out the cut for a block boundary this leadership authored — or `false` if the boundary is
     * not its.
     *
     * Unlike [applyAuthored] this is a responsibility rather than an optimisation: a cut is local state
     * as of exactly this position, so the boundary's author is the only node whose state that is.
     * Declining is what leaves the applier holding records until the upload arrives.
     */
    suspend fun takeCut(record: Log.Record<ReplicaMessage>, msg: ReplicaMessage.BlockBoundary): Boolean
}

/**
 * A higher-term record read back on our own replica log: a newer leader has superseded us. Raised from
 * the apply path to fail the term cleanly — not a query-facing fault, so it doesn't poison the watchers;
 * the transport re-follows on the next rebalance. See #5817.
 */
internal class LeaderSupersededException(message: String) : RuntimeException(message)
