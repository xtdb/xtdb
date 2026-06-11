package xtdb.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * Launches a child coroutine that runs [body], then runs [cleanup] under [NonCancellable].
 *
 * Guarantees:
 * - [cleanup] runs exactly once, after the body — whether the body returned, threw, was
 *   cancelled mid-flight, or was cancelled before it started (the coroutine starts
 *   [CoroutineStart.ATOMIC]; the [ensureActive] keeps default-start semantics for the body).
 * - The Job doesn't reach a final state until cleanup has returned, so a parent's
 *   `cancelAndJoin` waits for it — the property [Job.invokeOnCompletion] doesn't give you.
 *
 * Intended for cleanup of resources that exist before the launch. If the body acquires its
 * own resources, use `.use`/`try/finally` inside a plain [launch] instead.
 *
 * Workers the body launches are children of the hosting Job, not of the body statement —
 * fence them with `supervisorScope`/`coroutineScope`, or they race the cleanup.
 *
 * If [cleanup] throws, the Job completes exceptionally and the exception surfaces via the
 * scope's [kotlinx.coroutines.CoroutineExceptionHandler].
 *
 * [body] defaults to [awaitCancellation] for the "exists only to run cleanup on cancel" shape.
 *
 * Rationale and the underlying kotlinx semantics: `dev/doc/coroutines.adoc` / xtdb#5703.
 */
fun CoroutineScope.launchWithCleanup(
    context: CoroutineContext = EmptyCoroutineContext,
    cleanup: suspend () -> Unit,
    body: suspend CoroutineScope.() -> Unit = { awaitCancellation() }
): Job = launch(context, start = CoroutineStart.ATOMIC) {
    try {
        ensureActive()
        body()
    } finally {
        withContext(NonCancellable) { cleanup() }
    }
}
