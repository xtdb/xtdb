package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.future
import xtdb.api.TransactionResult
import xtdb.api.log.Watchers.Event.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.time.Duration.Companion.seconds

class Watchers(currentMsgId: MessageId) : AutoCloseable {
    private class Watcher(val msgId: MessageId, val onDone: CompletableDeferred<TransactionResult?>)

    @Volatile
    var currentMsgId = currentMsgId
        private set

    private val scope = CoroutineScope(Dispatchers.Default)

    var exception: IngestionStoppedException? = null
        private set

    private sealed interface Event {
        data class Notify(val msgId: MessageId, val result: TransactionResult?) : Event
        data class NotifyException(val msgId: MessageId, val exception: Throwable) : Event
        data class NewWatcher(val watcher: Watcher) : Event
    }

    private val channel = Channel<Event>(Channel.UNLIMITED)

    private val watchers = PriorityBlockingQueue<Watcher>(16) { a, b -> a.msgId.compareTo(b.msgId) }

    private suspend fun processEvents() {
        for (event in channel) {
            when (event) {
                is Notify -> {
                    check(event.msgId > currentMsgId)

                    currentMsgId = event.msgId
                    for (watcher in watchers) {
                        if (watcher.msgId > event.msgId) break
                        watchers.remove(watcher)
                        watcher.onDone.complete(event.result)
                    }
                }

                is NotifyException -> {
                    val ex = event.exception
                        .let { it as? IngestionStoppedException ?: IngestionStoppedException(event.msgId, it) }
                        .also { exception = it }

                    watchers.forEach { it.onDone.completeExceptionally(ex) }
                    watchers.clear()
                }

                is NewWatcher -> {
                    val watcher = event.watcher

                    val ex = exception

                    when {
                        ex != null -> watcher.onDone.completeExceptionally(ex)
                        currentMsgId >= watcher.msgId -> watcher.onDone.complete(null)
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

    fun notify(msgId: MessageId, result: TransactionResult?) {
        channel.trySend(Notify(msgId, result)).getOrThrow()
    }

    fun notify(msgId: MessageId, exception: Throwable) {
        channel.trySend(NotifyException(msgId, exception)).getOrThrow()
    }

    internal suspend fun await0(msgId: MessageId): TransactionResult? {
        exception?.let { throw it }
        if (currentMsgId >= msgId) return null

        val res = CompletableDeferred<TransactionResult?>()
        channel.trySend(NewWatcher(Watcher(msgId, res))).getOrThrow()

        return res.await()
    }

    fun awaitAsync(msgId: MessageId) = scope.future { await0(msgId) }

    override fun toString() =
        "(Watchers {watcherCount=${watchers.size}, firstWatcherMsgId=${watchers.firstOrNull()?.msgId}, currentMsgId=$currentMsgId, exception=$exception)})"
}