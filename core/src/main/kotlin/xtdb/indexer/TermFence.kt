package xtdb.indexer

import xtdb.api.DatabaseName
import xtdb.api.error.Conflict
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

    fun checkUnfenced(term: Long) {
        val maxTerm = highestSeen
        if (maxTerm > term)
            throw Conflict(
                "[$dbName] leader term ${LeaderTerm.format(term)} is already fenced by " +
                        "${LeaderTerm.format(maxTerm)} on the replica log — the leader-election counter " +
                        "has regressed (a recreated Kafka consumer group, or a restarted local log), so " +
                        "bump the log's termEpoch above ${LeaderTerm.epochOf(maxTerm)}",
                "xtdb/leader-term-fenced",
                mapOf(
                    "db-name" to dbName,
                    "term" to LeaderTerm.format(term),
                    "fenced-by" to LeaderTerm.format(maxTerm),
                ),
            )
    }
}
