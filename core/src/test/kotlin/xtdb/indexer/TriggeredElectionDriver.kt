package xtdb.indexer

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.SelectBuilder
import kotlin.time.Duration

/**
 * Fires a leader's assertion on demand rather than on a clock — which is what lets a seeded simulation, with
 * no clock to offer, run the path at all.
 *
 * [capacity] is the trigger channel's, and picks between the two ways a test drives it: [Channel.RENDEZVOUS]
 * where a send should land only once the appender has looped back and re-armed, [Channel.CONFLATED] where the
 * request should wait until it next does.
 *
 * The election timeout is never reached, so leadership still moves only where the test makes a poll come back
 * empty.
 */
internal class TriggeredElectionDriver(capacity: Int = Channel.RENDEZVOUS) : ElectionDriver {

    val trigger = Channel<Unit>(capacity)

    /** Assertions this node actually emitted, so a test that meant to exercise the path can say whether it did. */
    var assertsFired = 0
        private set

    fun requestAssert() {
        trigger.trySend(Unit)
    }

    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) =
        trigger.onReceive { assertsFired++; body() }

    override fun electionTimeout() = Duration.INFINITE
}
