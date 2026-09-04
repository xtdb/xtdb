package xtdb.indexer

import xtdb.api.log.LeaderTerm

/**
 * The highest leader term seen on one partition's replica log, and the read-side fence over it: a
 * record below the highest was written by a leader the log has since moved past, so every reader
 * discards it (#5817).
 *
 * Lives for the partition rather than for a role. A role change opens a fresh follower, and a fence
 * seeded afresh from the persisted block boundary would forget every term written since the last
 * block flush — so the same term could be admitted twice, once either side of a role change.
 *
 * Threading: the partition's single replica-log reader both folds and reads the fence, so nothing in
 * production races here; [highestSeen] is volatile because a test observes it from another thread.
 */
class TermFence(seed: Long) {

    /** What a record's term makes of the record, and of whoever wrote it. */
    enum class Admission {
        /** Written by a leader the log has moved past. Discard it, but still advance the consume position. */
        FENCED,

        /** Apply it. */
        ADMITTED,

        /** Apply it, and it conferred leadership on whoever wrote it — nothing at or above its term precedes it. */
        CONFERRING
    }

    @Volatile
    var highestSeen: Long = seed
        private set

    /**
     * Folds [term] into the highest seen, and says what the record carrying it is.
     *
     * Deciding and folding in are one operation because the verdict is against the highest term seen
     * *strictly before* this record: a caller that folded first would have nothing left to compare
     * against, and could not tell a claim that won from one that lost.
     *
     * [LeaderTerm.NONE] is never fenced and never confers, so a not-yet-upgraded leader's writes are
     * still applied during a mixed-version window without being read as an election.
     */
    fun admit(term: Long): Admission {
        if (term == LeaderTerm.NONE) return Admission.ADMITTED

        val seenBefore = highestSeen
        if (term < seenBefore) return Admission.FENCED

        highestSeen = term
        return if (term > seenBefore) Admission.CONFERRING else Admission.ADMITTED
    }
}
