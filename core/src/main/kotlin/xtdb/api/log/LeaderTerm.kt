package xtdb.api.log

/**
 * A leader term orders the leaders of a database's replica log, so that a superseded leader's writes
 * are discarded on the read side (#5817).
 *
 * A term comes from the log it fences: a claimant appends its claim one above the highest term that log
 * has been seen to reach, so terms are monotonic by construction and floored by the persisted block
 * boundary every participant seeds from. Nothing can reset it, and nothing outside the log can order two
 * of them.
 */
object LeaderTerm {
    /**
     * The unset term, and the bottom of the ordering: carried by a record written before terms
     * existed. Never fenced, so a mixed-version window doesn't discard a not-yet-upgraded leader's
     * writes — proto3's scalar default yields this for those records for free.
     */
    const val NONE = 0L
}
