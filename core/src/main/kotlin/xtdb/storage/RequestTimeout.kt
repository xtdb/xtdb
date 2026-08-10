package xtdb.storage

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import xtdb.api.error.Unavailable
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/**
 * The bound on a single object-store request.
 *
 * Generous by design, and deliberately not configurable: this is a watchdog on a client that has
 * accepted a request it will never answer (#5850, #5218), not a latency budget. No healthy single
 * request comes near it, and a value low enough to be worth tuning would start failing healthy
 * flushes. `var` only so tests can shorten it.
 */
internal var requestTimeout: Duration = 10.minutes

/**
 * Runs [body] under [requestTimeout], reporting a breach as [Unavailable].
 *
 * The bound has to sit *inside* the coroutine that issues the request, so that a breach cancels the
 * call and the client tears the request down. A bound applied from outside — `orTimeout` on a future,
 * a timeout held by whoever is awaiting the result — only stops us waiting, and leaves the request
 * running with nothing left to complete it.
 *
 * Rethrown as an ordinary exception because [TimeoutCancellationException] is a `CancellationException`,
 * which the block-write path above here reads as "we're shutting down" and discards — the same silence
 * this exists to break.
 */
internal suspend fun <T> withRequestTimeout(desc: String, body: suspend () -> T): T =
    try {
        withTimeout(requestTimeout) { body() }
    } catch (e: TimeoutCancellationException) {
        throw Unavailable(
            "object store did not respond within $requestTimeout: $desc",
            "xtdb/object-store-timeout", cause = e
        )
    }
