package xtdb.indexer

/**
 * The highest leader term seen on one partition's replica log, and the read-side fence over it: a record below the highest was written by a leader the log has since moved past, so every reader discards it (#5817).
 *
 * Lives for the partition rather than for a role.
 * A fence seeded afresh at each role change would forget every term written since the last block flush, so the same term could be admitted twice.
 *
 * Threading: [admit] is called by the partition's replica-log tail alone, in log order, so there is one writer for the fence's whole life.
 * [highestSeen] is volatile because tests observe it from another thread.
 */
class TermFence(seed: Long) {

    enum class Admission {
        /** Below a term already seen, so written by a leader the log has since moved past. */
        FENCED,

        /** At the highest term seen — the ordinary verdict on a leader's own writes. */
        ADMITTED,

        /** Above every term before it, so it conferred leadership on whoever wrote it. */
        CONFERRING
    }

    @Volatile
    var highestSeen: Long = seed
        private set

    /**
     * Folds [term] into the highest seen, and says what the record carrying it came to.
     *
     * Deciding and folding are one operation because the verdict is against the highest term seen *strictly before* this record: a caller that folded first would have nothing left to compare against.
     */
    fun admit(term: Long): Admission {
        val seenBefore = highestSeen

        return when {
            term < seenBefore -> Admission.FENCED
            term == seenBefore -> Admission.ADMITTED

            else -> {
                highestSeen = term
                Admission.CONFERRING
            }
        }
    }

    /**
     * Whether [term] is at or above the highest seen, without folding it in.
     *
     * For a record the fence must not learn a term from, because its position in the log does not bound where it is applied: the `BlockUploaded` closing a block is applied ahead of the records that block was holding, so folding its term would move the fence under them before they drain.
     *
     * A `BlockUploaded` reaches its role whatever this returns, because the one closing a block is matched on block index rather than on term.
     * The role drops the ones this refuses, which close nothing.
     */
    fun permits(term: Long) = term >= highestSeen
}
