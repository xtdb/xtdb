package xtdb.indexer

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.onTimeout
import xtdb.api.log.InMemoryLog
import xtdb.api.log.LocalLog
import xtdb.api.log.Log
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The two waits leadership turns on — a leader's before it asserts, a follower's before it claims — so a test sets them together rather than arranging a clock.
 *
 * The timeout arms into the caller's own select rather than being handed back: `SelectClause0` is sealed, so anything returnable would need a coroutine or a channel behind it.
 */
interface ElectionDriver {

    fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R)

    /**
     * How long to poll the replica log for.
     * Drawn per call: that redraw is the randomisation two candidates need to converge on one of them.
     */
    fun electionTimeout(): Duration
}

class RealElectionDriver(
    private val assertInterval: Duration = 1.seconds,
    private val random: Random = Random.Default,
) : ElectionDriver {

    /** Times the election off the log it will be run on: what an election has to outlast is whatever stands between an append and a follower seeing it — microseconds in-process, a network round-trip and a partition-leader election on Kafka. */
    constructor(replicaLog: Log<*>) : this(
        when (replicaLog) {
            is InMemoryLog, is LocalLog -> 100.milliseconds
            else -> 1.seconds
        }
    )

    // Derived rather than given: what an election timeout must outlast is a run of asserts, so it is the ratio that has to hold, and two independently-set numbers drift until it doesn't.
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
 * Never asserts, and never runs a poll down to a claim, for a test that pins the replica log's message sequence.
 * A sole node on a block-free database still claims once, that claim being taken without reading.
 */
object NoAssertElectionDriver : ElectionDriver {
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) = Unit
    override fun electionTimeout() = Duration.INFINITE
}
