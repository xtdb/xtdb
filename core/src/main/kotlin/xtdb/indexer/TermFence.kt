package xtdb.indexer

import xtdb.api.log.LeaderTerm

/**
 * The highest leader term seen on one partition's replica log, and the read-side fence over it: a
 * record below the highest was written by a leader the log has since moved past, so every reader
 * discards it (#5817).
 *
 * Lives for the partition rather than for a role. A role change opens a fresh follower, and a fence
 * seeded afresh from the persisted block boundary would forget every term written since the last
 * block flush — so the same term could be admitted twice, once either side of a demote.
 *
 * Threading: [admit] is called only from the partition's single replica-log reader, while [highest]
 * is read from the transition coroutine, hence the volatile.
 */
class TermFence(seed: Long) {

    @Volatile
    private var highestSeen: Long = seed

    /** The highest term seen so far, at or above the seed. */
    val highest: Long get() = highestSeen

    /**
     * Folds [term] into the highest seen, and says whether the record carrying it should be processed.
     *
     * Deciding and folding in are one operation because the verdict is against the highest term seen
     * *strictly before* this record: a caller that folded first would have nothing left to compare
     * against.
     *
     * [LeaderTerm.NONE] is never fenced, so a not-yet-upgraded leader's writes are still applied during
     * a mixed-version window; only stamped terms fence each other.
     */
    fun admit(term: Long): Boolean {
        if (term == LeaderTerm.NONE) return true

        val seenBefore = highestSeen
        if (term < seenBefore) return false

        highestSeen = term
        return true
    }
}
