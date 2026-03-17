package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onClosed
import xtdb.api.TransactionResult
import xtdb.api.log.Watchers.Event.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration.Companion.seconds


class Watchers @JvmOverloads constructor(
    currentMsgId: MessageId,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : AutoCloseable {

    private class Watcher(val msgId: MessageId, val cont: CancellableContinuation<TransactionResult?>)

    @Volatile
    var currentMsgId = currentMsgId
        private set

    @Volatile
    var exception: IngestionStoppedException? = null
        private set

    private sealed interface Event {
        data class Notify(val msgId: MessageId, val result: TransactionResult?) : Event
        data class NotifyException(val msgId: MessageId, val exception: Throwable) : Event
        data class NewWatcher(val watcher: Watcher) : Event
        data class Sync(val cont: CancellableContinuation<MessageId>) : Event
    }

    private val channel = Channel<Event>(Channel.UNLIMITED, onUndeliveredElement = { ev ->
        when (ev) {
            is NewWatcher -> ev.watcher.cont.cancel()
            is Sync -> ev.cont.cancel()
            else -> Unit
        }
    })

    private val watchers = PriorityBlockingQueue<Watcher>(16) { a, b -> a.msgId.compareTo(b.msgId) }

    private suspend fun processEvents() {
        try {
            for (event in channel) {
                when (event) {
                    is Notify -> {
                        check(event.msgId > currentMsgId)

                        currentMsgId = event.msgId
                        for (watcher in watchers) {
                            if (watcher.msgId > event.msgId) break
                            watchers.remove(watcher)
                            watcher.cont.resume(event.result?.takeIf { it.txId == watcher.msgId })
                        }
                    }

                    is NotifyException -> {
                        val ex = event.exception
                            .let { it as? IngestionStoppedException ?: IngestionStoppedException(event.msgId, it) }
                            .also { exception = it }

                        watchers.forEach { it.cont.resumeWithException(ex) }
                        watchers.clear()
                    }

                    is NewWatcher -> {
                        val watcher = event.watcher

                        val ex = exception

                        when {
                            ex != null -> watcher.cont.resumeWithException(ex)
                            currentMsgId >= watcher.msgId -> watcher.cont.resume(null)
                            else -> watchers.add(watcher)
                        }
                    }

                    is Sync -> {
                        event.cont.resume(currentMsgId)
                    }
                }
            }
        } finally {
            for (watcher in watchers) {
                watcher.cont.cancel()
            }

            watchers.clear()
        }
    }

    private val scope = CoroutineScope(coroutineContext)
    private val processJob = scope.launch { processEvents() }

    override fun close() {
        runBlocking {
            withTimeout(5.seconds) {
                channel.cancel()
                processJob.cancelAndJoin()
            }
        }
    }

    suspend fun notify(msgId: MessageId, result: TransactionResult?) {
        channel.send(Notify(msgId, result))
    }

    suspend fun notify(msgId: MessageId, exception: Throwable) {
        channel.send(NotifyException(msgId, exception))
    }

    suspend fun await(msgId: MessageId): TransactionResult? {
        exception?.let { throw it }
        if (currentMsgId >= msgId) return null

        return suspendCancellableCoroutine { cont ->
            channel.trySend(NewWatcher(Watcher(msgId, cont)))
                .onClosed { cont.cancel() }
        }
    }

    suspend fun sync(): MessageId =
        suspendCancellableCoroutine { cont ->
            channel.trySend(Sync(cont))
                .onClosed { cont.cancel() }
        }

    override fun toString() =
        "(Watchers {watcherCount=${watchers.size}, firstWatcherMsgId=${watchers.firstOrNull()?.msgId}, currentMsgId=$currentMsgId, exception=$exception)})"
}
