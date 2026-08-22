package xtdb.api.log

import xtdb.api.error.Incorrect
import java.time.Duration

/**
 * The timeouts leadership over a log is decided by. See `allium/log-processor-lifecycle.allium` sections
 * 5 and 7 — what the spec fixes is the *relations* between these rather than the values, and
 * [claimTimeout] exceeding [electionTimeoutMax] is the load-bearing one: it is what stops a claimant
 * whose reads are not arriving from resetting its peers' stopwatches faster than they can run down.
 *
 * The spread between min and max has to be comfortably wider than the interval at which empty reads
 * are reported ([Log.tailPollDuration]), because the stopwatch advances one reporting interval at a
 * time — two followers whose draws differ by less than one interval tip over on the same read however
 * far apart their timeouts nominally are.
 *
 * Each log supplies its own values ([Log.electionConfig]): the in-process logs elect in milliseconds,
 * because no second participant can exist to contend with, while a shared log's timeouts have to
 * absorb real scheduling and delivery delay.
 *
 * [assertionInterval] is how often a leader with nothing else to write asserts leadership on an idle
 * log, and must sit well below [electionTimeoutMin] so a healthy leader's silence never reaches a
 * follower's timeout. It is null for a log with a single participant — there is no follower to
 * reassure, so an idle leader appends nothing.
 */
data class ElectionConfig(
    val electionTimeoutMin: Duration = Duration.ofSeconds(6),
    val electionTimeoutMax: Duration = Duration.ofSeconds(12),
    val claimTimeout: Duration = Duration.ofSeconds(30),
    val abandonBackoffFactor: Int = 2,
    val assertionInterval: Duration? = Duration.ofSeconds(1),
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

        if (assertionInterval != null && assertionInterval >= electionTimeoutMin)
            throw Incorrect(
                "assertion interval must sit below the election timeout minimum, so a healthy " +
                        "leader's silence never reaches a follower's timeout",
                "xtdb/assertion-interval-invalid"
            )
    }
}
