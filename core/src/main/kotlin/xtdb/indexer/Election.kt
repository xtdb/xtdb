package xtdb.indexer

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectClause1
import xtdb.api.log.ElectionConfig
import xtdb.types.MessageId
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import kotlin.random.Random

/**
 * What a participant is waiting for the log to stay quiet long enough to justify.
 *
 * One per threshold, so every duration lives in the driver and none in the participant.
 */
enum class Quiet {
    /** Long enough that nobody appears to be leading, so this participant may claim. */
    ELECTION,

    /** Long enough that a claim this participant made would have come back, so it is not going to. */
    CLAIM_VERDICT,
}

/**
 * Decides when the log has been quiet long enough to act on, and reports it as an event.
 *
 * Every duration, the randomised spread and the backoff live here, so a participant states what it is
 * waiting for, is poked with what it read, and answers [onTimeout] from whatever state it is in.
 *
 * The pokes are what make it safe. A driver run off a clock would let a participant's own slowness stand
 * as a verdict on somebody else's health — and worst of all for a participant being delivered nothing,
 * which is the one that must never claim. Poked by reads, a participant that is not being read to
 * advances no threshold at all.
 *
 * Not thread-safe, and does not need to be: it is poked and answered on one loop at a time, and a role
 * change joins the old loop before the next one starts.
 */
interface ElectionDriver {
    /** Wait afresh for [quiet], from now. */
    fun await(quiet: Quiet)

    /**
     * Wait afresh for [Quiet.ELECTION], over a range widened for each successive call.
     *
     * A participant that cannot reach a verdict returns to the log less and less often rather than on a
     * fixed cadence, so several of them interleaving thin out rather than compounding — which is what
     * keeps them from holding every healthy peer short of its own timeout.
     */
    fun backOff()

    /** Wait for nothing: no threshold this driver holds applies to the participant's current state. */
    fun idle()

    /** A record was read and has been handled, which defers everything this driver is waiting for. */
    fun onRecord()

    /** A read came back with nothing, which is the only evidence the log is quiet. */
    fun onEmptyRead()

    /** Armed by the participant's loop; fires once the awaited quiet has been observed. */
    val onTimeout: SelectClause1<Quiet>
}

/**
 * Where the follower's read path reports what its replica log delivered.
 *
 * Every conclusion about leadership is drawn from a read — one that found a record, or one that found
 * nothing — so these two calls are the whole of what election needs from the reader. Both are made on
 * the follower's own work coroutine, which is the loop that answers [ElectionDriver.onTimeout].
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

    /** A read came back with nothing. */
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
 * The [ElectionDriver] a deployed node runs: quiet is the gap between this participant's last record and
 * its last read, so a threshold over it is crossed on a read or not at all.
 */
class QuietDriver(
    private val config: ElectionConfig,
    private val instantSource: InstantSource,
    private val rand: Random,
) : ElectionDriver {

    private var lastReadAt: Instant = instantSource.instant()
    private var lastRecordAt: Instant = lastReadAt

    private var awaiting: Quiet? = null

    /** Consecutive [backOff]s, each widening the range the election timeout is drawn from. */
    private var backoffs: Int = 0

    private var threshold: Duration = drawElectionTimeout(scale = 1)

    // Conflated, and delivered rather than returned, because the poke and the answer are the same loop: a
    // rendezvous would deadlock, and a second crossing before the first is answered says nothing new.
    private val timeouts = Channel<Quiet>(Channel.CONFLATED)

    override val onTimeout get() = timeouts.onReceive

    private fun drawElectionTimeout(scale: Int): Duration {
        val min = config.electionTimeoutMin.multipliedBy(scale.toLong())
        val max = config.electionTimeoutMax.multipliedBy(scale.toLong())
        val spread = max.minus(min).toNanos()
        return min.plusNanos(if (spread > 0) rand.nextLong(spread + 1) else 0)
    }

    // Only the election timeout is drawn from a range: two claimants have to be separated, where a
    // claimant waiting on its own claim is racing nobody.
    private fun thresholdFor(quiet: Quiet, scale: Int) = when (quiet) {
        Quiet.ELECTION -> drawElectionTimeout(scale)
        Quiet.CLAIM_VERDICT -> config.claimTimeout
    }

    override fun await(quiet: Quiet) {
        // A wait that reached a conclusion is evidence reads are arriving, which is the only thing backOff
        // backs away from — so a lost election clears it as surely as a won one.
        backoffs = 0
        restart(quiet, scale = 1)
    }

    override fun backOff() {
        backoffs += 1
        restart(Quiet.ELECTION, scale = 1 + config.abandonBackoffFactor * backoffs)
    }

    override fun idle() {
        awaiting = null
    }

    private fun restart(quiet: Quiet, scale: Int) {
        val now = instantSource.instant()
        lastReadAt = now
        lastRecordAt = now
        awaiting = quiet
        threshold = thresholdFor(quiet, scale)
        // A crossing observed under the old wait is no longer a fact about this one.
        timeouts.tryReceive()
    }

    override fun onRecord() {
        val now = instantSource.instant()
        lastReadAt = now
        lastRecordAt = now
        timeouts.tryReceive()
    }

    override fun onEmptyRead() {
        lastReadAt = instantSource.instant()

        val awaiting = this.awaiting ?: return
        if (Duration.between(lastRecordAt, lastReadAt) >= threshold) timeouts.trySend(awaiting)
    }
}
