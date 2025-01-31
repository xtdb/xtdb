package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.future
import xtdb.api.TransactionResult
import xtdb.api.log.Watchers.Event.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.time.Duration.Companion.seconds

class Watchers(currentOffset: LogOffset) : AutoCloseable {
    private class Watcher(val offset: LogOffset, val onDone: CompletableDeferred<TransactionResult?>)

    @Volatile
    var currentOffset = currentOffset
        private set

    private val scope = CoroutineScope(Dispatchers.Default)

    var exception: IngestionStoppedException? = null
        private set

    private sealed interface Event {
        data class Notify(val offset: LogOffset, val result: TransactionResult?) : Event
        data class NotifyException(val offset: LogOffset, val exception: Throwable) : Event
        data class NewWatcher(val watcher: Watcher) : Event
    }

    private val channel = Channel<Event>(Channel.UNLIMITED)

    private val watchers = PriorityBlockingQueue<Watcher>(16) { a, b -> a.offset.compareTo(b.offset) }

    private suspend fun processEvents() {
        for (event in channel) {
            when (event) {
                is Notify -> {
                    check(event.offset > currentOffset)

                    currentOffset = event.offset
                    for (watcher in watchers) {
                        if (watcher.offset > event.offset) break
                        watchers.remove(watcher)
                        watcher.onDone.complete(event.result)
                    }
                }

                is NotifyException -> {
                    val ex = IngestionStoppedException(event.exception).also { exception = it }

                    watchers.forEach { it.onDone.completeExceptionally(ex) }
                    watchers.clear()
                }

                is NewWatcher -> {
                    val watcher = event.watcher

                    val ex = exception

                    when {
                        ex != null -> watcher.onDone.completeExceptionally(ex)
                        currentOffset >= watcher.offset -> watcher.onDone.complete(null)
                        else -> watchers.add(watcher)
                    }
                }
            }
        }
    }

    init {
        scope.launch { processEvents() }
    }

    override fun close() {
        channel.close()
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }

    fun notify(offset: LogOffset, result: TransactionResult?) {
        channel.trySend(Notify(offset, result)).getOrThrow()
    }

    fun notify(offset: LogOffset, exception: Throwable) {
        channel.trySend(NotifyException(offset, exception)).getOrThrow()
    }

    internal suspend fun await0(offset: LogOffset): TransactionResult? {
        exception?.let { throw it }
        if (currentOffset >= offset) return null

        val res = CompletableDeferred<TransactionResult?>()
        channel.trySend(NewWatcher(Watcher(offset, res))).getOrThrow()

        return res.await()
    }

    fun awaitAsync(offset: LogOffset) = scope.future { await0(offset) }

    override fun toString() =
        "(Watchers {watcherCount=${watchers.size}, firstWatcherOffset=${watchers.firstOrNull()?.offset}, currentOffset=$currentOffset, exception=$exception)})"
}