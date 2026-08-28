package xtdb.indexer

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.onTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Clauses are armed into the caller's own select rather than handed back: [kotlinx.coroutines.selects.SelectClause0] is sealed, so anything returnable would need a coroutine or a channel behind it. */
interface ElectionDriver {

    fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R)
}

class RealElectionDriver(private val assertInterval: Duration = 1.seconds) : ElectionDriver {

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) =
        onTimeout(assertInterval, body)
}

/** For a test that pins the replica log's message sequence, where an assertion arriving mid-run would make the sequence depend on how long the test took. */
object NoAssertElectionDriver : ElectionDriver {
    override fun <R> SelectBuilder<R>.onAssertTimeout(body: suspend () -> R) = Unit
}
