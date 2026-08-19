package xtdb.indexer

import kotlinx.coroutines.CompletableDeferred
import xtdb.api.error.Incorrect
import xtdb.types.MessageId
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import kotlin.random.Random

/**
 * The timeouts leadership is decided by. See `allium/log-processor-lifecycle.allium` section 5 — what
 * the spec fixes is the *relations* between these rather than the values, and [claimTimeout] exceeding
 * [electionTimeoutMax] is the load-bearing one: it is what stops a claimant whose reads are not
 * arriving from resetting its peers' stopwatches faster than they can run down.
 *
 * The spread between min and max has to be comfortably wider than the interval at which empty reads
 * are reported ([xtdb.api.log.Log.TAIL_POLL_DURATION]), because the stopwatch advances one reporting
 * interval at a time — two followers whose draws differ by less than one interval tip over on the same
 * read however far apart their timeouts nominally are.
 */
data class ElectionConfig(
    val electionTimeoutMin: Duration = Duration.ofSeconds(6),
    val electionTimeoutMax: Duration = Duration.ofSeconds(12),
    val claimTimeout: Duration = Duration.ofSeconds(30),
    val abandonBackoffFactor: Int = 2,
) {
    init {
        if (electionTimeoutMin <= Duration.ZERO)
            throw Incorrect("election timeout minimum must be positive", "xtdb/election-timeout-invalid")

        if (electionTimeoutMax < electionTimeoutMin)
            throw Incorrect("election timeout maximum is below its minimum", "xtdb/election-timeout-invalid")

        if (claimTimeout <= electionTimeoutMax)
            throw Incorrect(
                "claim timeout must exceed the election timeout maximum, so that a claimant which " +
                        "cannot read its claim back leaves a quiet window longer than any peer's timeout",
                "xtdb/claim-timeout-invalid"
            )

        if (abandonBackoffFactor < 0)
            throw Incorrect("abandon backoff factor cannot be negative", "xtdb/abandon-backoff-invalid")
    }
}

/** The outcome of a claim, which is decided by the reader and acted on by the takeover. */
sealed interface Verdict {
    /** Nothing at or above the claim's term preceded it, so it conferred leadership. */
    data object Conferred : Verdict

    /** Something at or above the claim's term preceded it — an ordinary outcome, not a fault. */
    data object Lost : Verdict

    /** The claim did not come back within the claim timeout, so the claimant is no longer being read to. */
    data object Abandoned : Verdict
}

/**
 * One node's bid for leadership: a record it appended, carrying the term it claims.
 *
 * Its position is its identity, which is why nothing has to be written into the record to tell claims
 * apart, and why a participant holding at most one at a time needs no token to check a verdict against.
 * [verdict] completes on the reader's own coroutine and is awaited by the takeover, which is the only
 * work that cannot run there.
 */
class Claim(val term: Long, val msgId: MessageId) {
    val verdict = CompletableDeferred<Verdict>()
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
 * Not thread-safe, and does not need to be: it is read and written only from the node's single
 * replica-log reader.
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
