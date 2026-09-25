package xtdb.postgres

import xtdb.api.error.Incorrect
import kotlin.math.ceil
import kotlin.math.log2
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * How long to wait before reopening a replication stream that has failed, and when that wait starts over.
 *
 * The wait doubles with each consecutive failure up to [maxDelay], and carries jitter so that sources
 * reconnecting to the same upstream after a failover don't do so in lockstep.
 *
 * @property initialDelay must be positive.
 * @property maxDelay must be finite, and at least [initialDelay].
 * @property resetAfter how long a reopened stream has to go without another failure before the next
 *   failure counts as the first again.
 * @suppress
 */
data class ReconnectPolicy(
    val initialDelay: Duration = 1.seconds,
    val maxDelay: Duration = 30.seconds,
    val resetAfter: Duration = 10.minutes,
    val jitter: Double = 0.5,
    val random: Random = Random.Default,
) {
    init {
        if (!initialDelay.isPositive() || maxDelay < initialDelay || maxDelay.isInfinite())
            throw Incorrect(
                "A reconnect policy needs a positive initial delay no greater than a finite maximum, got $initialDelay and $maxDelay",
                errorCode = "xtdb.postgres/invalid-reconnect-policy",
            )
    }

    private val doublingsToCap = ceil(log2(maxDelay / initialDelay)).toInt()

    /**
     * The wait after the [failures]th consecutive failure: [initialDelay] doubled per failure up to [maxDelay],
     * less up to [jitter] of that. No failures owes no wait.
     */
    fun delayAfter(failures: Int): Duration {
        if (failures < 1) return Duration.ZERO

        val backoff = minOf(initialDelay * 2.0.pow(minOf(failures - 1, doublingsToCap)), maxDelay)
        return backoff - backoff * (jitter * random.nextDouble())
    }
}
