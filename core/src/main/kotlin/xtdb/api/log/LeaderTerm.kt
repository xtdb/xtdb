package xtdb.api.log

/**
 * A leader term orders the leaders of a database's replica log, so that a superseded leader's writes
 * are discarded on the read side (#5817).
 *
 * A term is claimed one above the highest a node has read from that same log, so the ordering is the
 * log's own and depends on nothing outside it — a plain monotone `Long`, compared as one everywhere it
 * is checked: the fence, the persisted block boundary, the proto field.
 */
object LeaderTerm {
    /**
     * The unset term, and the bottom of the ordering: carried by a record written before terms
     * existed. Never fenced, so a mixed-version window doesn't discard a not-yet-upgraded leader's
     * writes — proto3's scalar default yields this for those records for free.
     */
    const val NONE = 0L
}
