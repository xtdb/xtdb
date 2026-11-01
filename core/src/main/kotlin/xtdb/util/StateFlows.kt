package xtdb.util

import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map

fun <T, R> StateFlow<T>.mapState(f: (T) -> R): StateFlow<R> = object : StateFlow<R> {
    override val value get() = f(this@mapState.value)
    override val replayCache get() = listOf(value)

    override suspend fun collect(collector: FlowCollector<R>): Nothing {
        this@mapState.map(f).distinctUntilChanged().collect(collector)
        error("a StateFlow never completes")
    }
}
