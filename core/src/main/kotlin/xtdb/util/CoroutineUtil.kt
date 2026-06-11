package xtdb.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * Launches a child coroutine that runs [body], then runs [cleanup] under [NonCancellable].
 *
 * The cleanup is part of the launched Job's lifecycle: the Job doesn't reach a final state
 * until cleanup has returned, so a parent's `cancelAndJoin` waits for it. That's the
 * property [kotlinx.coroutines.Job.invokeOnCompletion] doesn't give you — see
 * `dev/doc/coroutines.adoc` / xtdb#5703.
 *
 * Cleanup runs after the body whether the body returned normally, threw, or was cancelled.
 * [NonCancellable] keeps cleanup running through cancellation requests that arrive while it
 * is in flight.
 *
 * If [cleanup] throws, the Job ends `CompletedExceptionally` and the exception surfaces via
 * the scope's [kotlinx.coroutines.CoroutineExceptionHandler].
 *
 * [body] defaults to [awaitCancellation] for the "exists only to run cleanup on cancel"
 * shape; pass a body for "do work, then clean up after."
 */
fun CoroutineScope.launchWithCleanup(
    cleanup: suspend () -> Unit,
    body: suspend CoroutineScope.() -> Unit = { awaitCancellation() }
): Job = launch {
    try {
        body()
    } finally {
        withContext(NonCancellable) { cleanup() }
    }
}
