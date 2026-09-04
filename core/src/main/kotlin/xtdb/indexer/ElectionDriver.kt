package xtdb.indexer

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.onTimeout
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The two waits leadership turns on — a leader's before it asserts, a follower's before it claims — so a
 * test sets them together rather than arranging a clock.
 *
 * Clauses are armed into the caller's own select rather than handed back: `onTimeout` is a
 * [SelectBuilder] extension, and [kotlinx.coroutines.selects.SelectClause0] is sealed, so the only
 * clauses we could return are ones obtained from a kotlinx primitive — each of which would need a
 * coroutine or a channel behind it.
 */
interface ElectionDriver {

    /**
     * This is the one thing here timed on a clock. Producing traffic concludes nothing about anybody;
     * it is drawing a conclusion *from* silence that has to be derived from what this node has read.
     */
    fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R)

    /**
     * How long to poll the replica log for. A poll of this length coming back empty is a claim, so the
     * draw is per-call: that redraw *is* the randomisation two candidates need to converge, and a poll
     * that returned records has already redrawn by the time the next one starts.
     */
    fun electionTimeout(): Duration
}

class RealElectionDriver(
    private val assertInterval: Duration = 1.seconds,
    private val random: Random = Random.Default,
) : ElectionDriver {

    // Five to ten assert intervals, so a healthy leader has to miss several before anyone challenges it.
    // Derived rather than given, because it is the ratio that has to hold: what an election timeout must
    // outlast is a run of asserts, and two independently-set numbers drift until it doesn't.
    // The floor is what stalls an assert without the leader being gone — on Kafka, a partition-leader
    // election, a broker or a node GC pause. The 2x spread is the randomisation two candidates converge on.
    private val electionTimeoutRange = assertInterval * 5..assertInterval * 10

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) =
        onTimeout(assertInterval, body)

    override fun electionTimeout() =
        random.nextLong(
            electionTimeoutRange.start.inWholeMilliseconds,
            electionTimeoutRange.endInclusive.inWholeMilliseconds + 1
        ).milliseconds
}

/**
 * Never asserts, and never runs a poll down to a claim.
 *
 * For a test that pins the replica log's message sequence, where an assertion or a second election
 * arriving mid-run would make the sequence depend on how long the test took. A sole node on a
 * block-free database still claims once, because that claim is taken without reading.
 */
object NoAssertElectionDriver : ElectionDriver {
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) = Unit
    override fun electionTimeout() = Duration.INFINITE
}
