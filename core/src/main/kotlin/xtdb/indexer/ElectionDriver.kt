package xtdb.indexer

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.onTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
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
}

class RealElectionDriver(private val assertInterval: Duration = 1.seconds) : ElectionDriver {

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) =
        onTimeout(assertInterval, body)
}

/**
 * Arms nothing, so a leader never asserts.
 *
 * For a test that pins the replica log's message sequence, where an assertion arriving mid-run would
 * make the sequence depend on how long the test took.
 */
object NoAssertElectionDriver : ElectionDriver {
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) = Unit
}
