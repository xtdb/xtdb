package xtdb.indexer

import xtdb.api.DatabaseName
import xtdb.api.log.LeaderTerm

/**
 * A leader term the replica log has already moved past, refused at the moment it is claimed.
 *
 * Distinguishable by type because the transport routes it: a cancellation there says the transition was
 * revoked and is dropped silently, where this reaches the operator. See #5817.
 */
internal class LeaderSupersededException(message: String) : RuntimeException(message)

/**
 * The highest leader term seen on one partition's replica log, and the read-side fence over it: a
 * record below the highest was written by a leader the log has since moved past, so every reader
 * discards it (#5817).
 *
 * Lives for the partition rather than for a role. A role change opens a fresh follower, and a fence
 * seeded afresh from the persisted block boundary would forget every term written since the last
 * block flush — so the same term could be admitted twice, once either side of a demote.
 *
 * Threading: [admit] is called by whichever role the partition's single replica-log reader is dispatching
 * to, one record at a time, and by the transition while both roles are down — so there is one writer at
 * any moment, though not one for the fence's whole life. [highestSeen] is read from the transition
 * coroutine, hence the volatile.
 */
class TermFence(private val dbName: DatabaseName, seed: Long) {

    @Volatile
    var highestSeen: Long = seed
        private set

    /**
     * Folds [term] into the highest seen, and says whether the record carrying it should be processed.
     *
     * Deciding and folding in are one operation because the verdict is against the highest term seen
     * *strictly before* this record: a caller that folded first would have nothing left to compare
     * against.
     */
    fun admit(term: Long): Boolean {
        val seenBefore = highestSeen
        if (term < seenBefore) return false

        highestSeen = term
        return true
    }

    /**
     * Refuse [term] where the log has already reached a higher one — every reader would discard what a
     * leader at [term] wrote, so it must not take leadership.
     *
     * The same refusal covers both ways the log gets above a claim, because the fence cannot tell them
     * apart and the operator needs the second named either way: a newer leader legitimately superseding
     * this one, and the election counter regressing underneath it. Refusing costs liveness only, never
     * safety, and it is a resignation rather than a fault — it MUST NOT reach the watchers, or a node that
     * merely failed to lead would leave a healthy database unqueryable (#5817).
     */
    fun checkUnfenced(term: Long) {
        val maxTerm = highestSeen
        if (maxTerm > term)
            throw LeaderSupersededException(
                "[$dbName] leader term ${LeaderTerm.format(term)} is fenced by " +
                        "${LeaderTerm.format(maxTerm)} on the replica log. Where the leader-election " +
                        "counter has regressed rather than a newer leader having superseded this one " +
                        "(a recreated Kafka consumer group, or a restarted local log), bump the log's " +
                        "termEpoch above ${LeaderTerm.epochOf(maxTerm)}"
            )
    }
}
