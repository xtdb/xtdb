package xtdb.indexer

import xtdb.api.log.ElectionConfig
import xtdb.types.MessageId
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import kotlin.random.Random

/**
 * Where the follower's read path reports what its replica log delivered, so the partition can decide
 * leadership from it.
 *
 * Every conclusion about leadership is drawn from a read — one that found a record, or one that found
 * nothing — so these two calls are the whole of what election needs from the reader. Both are made on
 * the follower's own work coroutine, which is the serialization point every decision runs on.
 */
interface Elector {
    /**
     * A record was read and has been handled. [termBefore] is the highest term seen strictly before it —
     * a claim's verdict turns on the prefix strictly before the claim, so it has to be captured before
     * the record's own term is folded in.
     *
     * MUST be called *after* the record is handled, not when it arrives: stamped on arrival, the time
     * spent processing accumulates as quiet, and a follower that took longer over a record than its
     * election timeout would claim against a leader that was healthy throughout.
     */
    suspend fun onRecord(termBefore: Long, msgId: MessageId)

    /** A read came back with nothing, which is the only evidence the log is quiet. */
    suspend fun onEmptyRead()
}

/**
 * One node's bid for leadership: a record it appended, carrying the term it claims.
 *
 * Its position is its identity, which is why nothing has to be written into the record to tell claims
 * apart, and why a participant holding at most one at a time needs no token to check a verdict against.
 */
class Claim(val term: Long, val msgId: MessageId)

/**
 * Asserts leadership on an idle log, so a healthy leader's silence never reaches a follower's timeout.
 *
 * Runs on the leader's replica *reader* rather than its work loop, so an upload in flight cannot silence
 * the leader — the assertion must not queue behind the leader's own indexing work, or a leader falls
 * silent exactly while it is doing the most and is read as absent.
 *
 * Every record read is somebody's assertion already, so only quiet costs anything — and the interval is
 * measured from this leader's own last append as well as from what it reads, so a leader whose reads
 * have stopped does not assert on every empty read.
 *
 * [interval] is null for a log with a single participant: there is no follower to reassure.
 */
class LeadershipAssertion(
    private val interval: Duration?,
    private val instantSource: InstantSource,
    private val assert: suspend () -> Unit,
) {
    private var lastSeenOrAssertedAt: Instant = instantSource.instant()

    fun onRecord() {
        lastSeenOrAssertedAt = instantSource.instant()
    }

    suspend fun onEmptyRead() {
        if (interval == null) return

        val now = instantSource.instant()
        if (Duration.between(lastSeenOrAssertedAt, now) >= interval) {
            assert()
            lastSeenOrAssertedAt = now
        }
    }
}

/**
 * How long this participant has been reading its log and finding nothing.
 *
 * The single quantity everything in election is timed off. Three thresholds read it — claim, give up on
 * a claim, resign — and it advances only across reads, never with the passing of time. That is what
 * stops a participant's own slowness becoming a conclusion about somebody else's health, and it is also
 * why no guard has to ask how far the log extends beyond what it handed over: a participant that is not
 * being delivered to cannot advance any threshold, so being unfed is not a route to claiming.
 *
 * Not thread-safe, and does not need to be: whichever role is live, exactly one loop reads the partition's
 * replica log at a time, and role changes join the old loop before the new one starts.
 */
class QuietStopwatch(
    private val config: ElectionConfig,
    private val instantSource: InstantSource,
    private val rand: Random,
) {
    private var lastReadAt: Instant = instantSource.instant()
    private var lastRecordAt: Instant = lastReadAt

    /** Consecutive claims abandoned without reaching a verdict. */
    var abandonedClaims: Int = 0
        private set

    private var electionTimeout: Duration = drawTimeout(scale = 1)

    /** Seeding both from the current instant is what stops a fresh participant claiming on its first read. */
    private fun drawTimeout(scale: Int): Duration {
        val min = config.electionTimeoutMin.multipliedBy(scale.toLong())
        val max = config.electionTimeoutMax.multipliedBy(scale.toLong())
        val spread = max.minus(min).toNanos()
        return min.plusNanos(if (spread > 0) rand.nextLong(spread + 1) else 0)
    }

    /**
     * A record was read and has been processed, which defers this participant's election.
     *
     * MUST be called *after* the record is handled, not when it arrives. Stamped on arrival, the time
     * spent processing accumulates as quiet: a follower that took longer over a record than its election
     * timeout would then claim on its next empty read, against a leader that was healthy throughout, and
     * the more so the busier it was.
     */
    fun onRecord() {
        val now = instantSource.instant()
        lastReadAt = now
        lastRecordAt = now
    }

    /** A read came back with nothing, which is the only evidence the log is quiet. */
    fun onEmptyRead() {
        lastReadAt = instantSource.instant()
    }

    val quietFor: Duration get() = Duration.between(lastRecordAt, lastReadAt)

    /** The log has been quiet long enough to justify an election. */
    val quietLongEnough: Boolean get() = quietFor >= electionTimeout

    /** This claimant has been reading long enough that its own claim should have come back. */
    val claimOverdue: Boolean get() = quietFor >= config.claimTimeout

    /**
     * Wait afresh from now, with a newly drawn timeout.
     *
     * For every return to following whose claim reached a verdict, or which never got a claim into the
     * log at all. Losing counts: the claim came back, which is evidence that reads are arriving, and
     * arriving reads are the only thing [backOff] backs away from.
     */
    fun restartWait() {
        abandonedClaims = 0
        resetWait(scale = 1)
    }

    /**
     * Wait afresh from now, over a range widened for each successive abandonment.
     *
     * A participant that cannot reach a verdict returns to the log less and less often rather than on a
     * fixed cadence, so several of them interleaving thin out rather than compounding — which is what
     * keeps them from holding every healthy peer short of its own timeout.
     */
    fun backOff() {
        abandonedClaims += 1
        resetWait(scale = 1 + config.abandonBackoffFactor * abandonedClaims)
    }

    private fun resetWait(scale: Int) {
        val now = instantSource.instant()
        lastReadAt = now
        lastRecordAt = now
        electionTimeout = drawTimeout(scale)
    }
}
