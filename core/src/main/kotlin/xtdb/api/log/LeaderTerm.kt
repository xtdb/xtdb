package xtdb.api.log

/**
 * A leader term orders the leaders of a database's replica log, so that a superseded leader's writes
 * are discarded on the read side (#5817).
 *
 * A term is claimed one above the highest a node has read from that same log, so the ordering is the
 * log's own and depends on nothing outside it.
 *
 * Packed like a [xtdb.types.MessageId]: with an epoch in the top bits, plain numeric ordering is already
 * lexicographic over (epoch, election), so every comparison site — the fence, the persisted block
 * boundary, the proto field — stays a `Long`. Adding one to a packed term raises the counter alone, so a
 * boundary persisted under an earlier epoch still orders below everything claimed after it.
 */
object LeaderTerm {
    // 15 bits, not 16: the epoch's top bit is the packed term's sign bit, and a negative term would
    // order *below* every other — raising the epoch would then do the exact opposite of its job.
    private const val EPOCH_LIMIT = 1L shl 15
    private const val ELECTION_LIMIT = 1L shl 48
    private const val ELECTION_MASK = ELECTION_LIMIT - 1

    /**
     * The unset term, and the bottom of the ordering: carried by a record written before terms
     * existed. Never fenced, so a mixed-version window doesn't discard a not-yet-upgraded leader's
     * writes — proto3's scalar default yields this for those records for free.
     */
    const val NONE = 0L

    @JvmStatic
    fun of(termEpoch: Int, election: Long): Long {
        require(termEpoch in 0 until EPOCH_LIMIT) { "Term epoch ($termEpoch) outside [0, $EPOCH_LIMIT)" }
        require(election in 0 until ELECTION_LIMIT) { "Election ($election) outside [0, $ELECTION_LIMIT)" }
        return (termEpoch.toLong() shl 48) + election
    }

    @JvmStatic
    fun epochOf(term: Long): Int = (term ushr 48).toInt()

    @JvmStatic
    fun electionOf(term: Long): Long = term and ELECTION_MASK

    /** Renders a term for humans — the packed value on its own is unreadable. */
    @JvmStatic
    fun format(term: Long) = "${epochOf(term)}.${electionOf(term)}"
}
