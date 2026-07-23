package xtdb.api.log

/**
 * A leader term orders the leaders of a database's replica log, so that a superseded leader's writes
 * are discarded on the read side (#5817).
 *
 * The election counter in the low bits comes from the transport's leader election — a Kafka
 * consumer-group `generationId`, or an in-process counter for the local logs — so it orders elections
 * *within* one incarnation of that election mechanism, but does not survive the mechanism being reset.
 * Kafka's coordinator deletes a consumer group once it is empty with no committed offsets, and we
 * commit none (the source-log position lives in the replica log, not in Kafka), so a cluster stopped
 * for longer than the coordinator's sweep interval comes back to a group whose first generation is 1.
 * The local logs' counter dies with the process. The epoch is the operator's declaration that such a
 * reset has happened.
 *
 * It has to be *declared* rather than derived from the log. The leader this fence exists to stop is
 * precisely one that can still reach the log while being out of touch with whatever elects leaders —
 * so it can still read. An epoch taken from the log (its end offset, the persisted boundary term, the
 * block index) would let that leader observe the progress of the leader which legitimately superseded
 * it and thereby claim a *higher* term, inverting the fence.
 *
 * Packed like a [xtdb.types.MessageId] and for the same reason: with the epoch in the top bits, plain
 * numeric ordering is already lexicographic over (epoch, election), so every comparison site — the
 * follower's fence, the persisted block boundary, the proto field — stays a `Long`.
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
