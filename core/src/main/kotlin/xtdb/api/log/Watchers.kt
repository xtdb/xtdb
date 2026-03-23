package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onClosed
import xtdb.api.TxId
import xtdb.api.TransactionResult
import xtdb.api.log.Watchers.Event.*
import java.util.Queue
import java.util.concurrent.PriorityBlockingQueue
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration.Companion.seconds

class Watchers @JvmOverloads constructor(
    latestTxId: TxId,
    latestSourceMsgId: MessageId,
    latestReplicaMsgId: MessageId,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : AutoCloseable {

    /**
     * Backward-compat constructor for source-only (no replica) setups.
     */
    @JvmOverloads
    constructor(
        latestSourceMsgId: MessageId, coroutineContext: CoroutineContext = Dispatchers.Default
    ) : this(
        latestTxId = latestSourceMsgId, latestSourceMsgId = latestSourceMsgId, latestReplicaMsgId = -1,
        coroutineContext
    )

    @Volatile
    var latestTxId: TxId = latestTxId
        private set

    @Volatile
    var latestSourceMsgId: MessageId = latestSourceMsgId
        private set

    @Volatile
    var latestReplicaMsgId: MessageId = latestReplicaMsgId
        private set

    @Volatile
    var exception: IngestionStoppedException? = null
        private set

    sealed interface Watcher {
        val cont: CancellableContinuation<*>
    }

    data class TxWatcher(val txId: TxId, override val cont: CancellableContinuation<TransactionResult?>) : Watcher
    data class SourceWatcher(val msgId: MessageId, override val cont: CancellableContinuation<Unit>) : Watcher
    data class ReplicaWatcher(val msgId: MessageId, override val cont: CancellableContinuation<Unit>) : Watcher
    data class SyncWatcher(override val cont: CancellableContinuation<Unit>) : Watcher

    private sealed interface Event {
        data class NotifyTx(
            // replica only nullable until SourceLogProcessor goes
            val result: TransactionResult, val srcMsgId: MessageId, val replicaMsgId: MessageId?
        ) : Event

        data class NotifyMsg(val srcMsgId: MessageId?, val replicaMsgId: MessageId?) : Event
        data class NotifyError(val exception: Throwable) : Event
        data class NewWatcher(val watcher: Watcher) : Event
    }

    private val channel = Channel<Event>(Channel.UNLIMITED, onUndeliveredElement = { ev ->
        if (ev is NewWatcher) ev.watcher.cont.cancel()
        else Unit
    })

    private val txWatchers = PriorityBlockingQueue<TxWatcher>(16) { a, b -> a.txId.compareTo(b.txId) }
    private val sourceWatchers = PriorityBlockingQueue<SourceWatcher>(16) { a, b -> a.msgId.compareTo(b.msgId) }
    private val replicaWatchers = PriorityBlockingQueue<ReplicaWatcher>(16) { a, b -> a.msgId.compareTo(b.msgId) }

    private fun Queue<out Watcher>.resumeWithException(ex: Throwable) {
        for (watcher in this) watcher.cont.resumeWithException(ex)
    }

    private fun Queue<out Watcher>.cancel() {
        for (watcher in this) watcher.cont.cancel()
        this.clear()
    }

    private fun handleNotifySource(srcMsgId: MessageId) {
        // >= not >: BlockBoundary can carry the same source msgId as the preceding ResolvedTx
        // when the block was triggered by isFull() (no FlushBlock in between)
        check(srcMsgId >= latestSourceMsgId) { "srcMsgId $srcMsgId < latestSourceMsgId $latestSourceMsgId" }
        if (srcMsgId == latestSourceMsgId) return
        latestSourceMsgId = srcMsgId

        for (watcher in sourceWatchers) {
            if (watcher.msgId > srcMsgId) break
            sourceWatchers.remove(watcher)
            watcher.cont.resume(Unit)
        }
    }

    private fun handleNotifyReplica(replicaMsgId: MessageId) {
        check(replicaMsgId > latestReplicaMsgId) { "replicaMsgId $replicaMsgId <= latestReplicaMsgId $latestReplicaMsgId" }
        latestReplicaMsgId = replicaMsgId

        for (watcher in replicaWatchers) {
            if (watcher.msgId > replicaMsgId) break
            replicaWatchers.remove(watcher)
            watcher.cont.resume(Unit)
        }
    }

    private fun handleNotifyTx(result: TransactionResult) {
        val txId = result.txId
        check(txId > latestTxId) { "txId $txId <= latestTxId $latestTxId" }
        latestTxId = txId

        for (watcher in txWatchers) {
            if (watcher.txId > txId) break
            txWatchers.remove(watcher)
            watcher.cont.resume(result.takeIf { it.txId == watcher.txId })
        }
    }

    private suspend fun processEvents() {
        try {
            for (event in channel) {
                when (event) {
                    is NotifyTx -> {
                        handleNotifyTx(event.result)
                        handleNotifySource(event.srcMsgId)
                        event.replicaMsgId?.let { handleNotifyReplica(it) }
                    }

                    is NotifyMsg -> {
                        event.srcMsgId?.let { handleNotifySource(it) }
                        event.replicaMsgId?.let { handleNotifyReplica(it) }
                    }

                    is NotifyError -> {
                        val ex = event.exception
                            .let { it as? IngestionStoppedException ?: IngestionStoppedException(null, it) }
                            .also { exception = it }

                        txWatchers.resumeWithException(ex)
                        sourceWatchers.resumeWithException(ex)
                        replicaWatchers.resumeWithException(ex)
                    }

                    is NewWatcher -> {
                        exception?.let { ex ->
                            event.watcher.cont.resumeWithException(ex)
                            continue
                        }

                        when (val watcher = event.watcher) {
                            is TxWatcher ->
                                if (latestTxId >= watcher.txId) watcher.cont.resume(null)
                                else txWatchers.add(watcher)

                            is SourceWatcher ->
                                if (latestSourceMsgId >= watcher.msgId) watcher.cont.resume(Unit)
                                else sourceWatchers.add(watcher)

                            is ReplicaWatcher ->
                                if (latestReplicaMsgId >= watcher.msgId) watcher.cont.resume(Unit)
                                else replicaWatchers.add(watcher)

                            is SyncWatcher -> watcher.cont.resume(Unit)
                        }
                    }
                }
            }
        } finally {
            txWatchers.cancel()
            sourceWatchers.cancel()
            replicaWatchers.cancel()
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

    // --- notify methods ---

    /** Source-only path (no replica) — advances tx + source watermarks. */
    suspend fun notifyTx(result: TransactionResult, srcMsgId: MessageId) {
        channel.send(NotifyTx(result, srcMsgId, replicaMsgId = null))
    }

    /** Leader/follower path — advances tx + source + replica watermarks. */
    suspend fun notifyTx(result: TransactionResult, srcMsgId: MessageId, replicaMsgId: MessageId) {
        channel.send(NotifyTx(result, srcMsgId, replicaMsgId))
    }

    suspend fun notifyMsg(srcMsgId: MessageId?, replicaMsgId: MessageId?) {
        channel.send(NotifyMsg(srcMsgId, replicaMsgId))
    }

    suspend fun notifyError(exception: Throwable) {
        channel.send(NotifyError(exception))
    }

    // --- await methods ---

    suspend fun awaitTx(txId: TxId): TransactionResult? {
        exception?.let { throw it }
        if (latestTxId >= txId) return null

        return suspendCancellableCoroutine { cont ->
            channel.trySend(NewWatcher(TxWatcher(txId, cont)))
                .onClosed { cont.cancel() }
        }
    }

    suspend fun awaitSource(srcMsgId: MessageId) {
        exception?.let { throw it }
        if (latestSourceMsgId >= srcMsgId) return

        suspendCancellableCoroutine { cont ->
            channel.trySend(NewWatcher(SourceWatcher(srcMsgId, cont)))
                .onClosed { cont.cancel() }
        }
    }

    suspend fun awaitReplica(replicaMsgId: MessageId) {
        exception?.let { throw it }
        if (latestReplicaMsgId >= replicaMsgId) return

        suspendCancellableCoroutine { cont ->
            channel.trySend(NewWatcher(ReplicaWatcher(replicaMsgId, cont)))
                .onClosed { cont.cancel() }
        }
    }

    /**
     * Flushes the event channel — ensures all prior notifications have been applied
     * before returning, so that [latestSourceMsgId]/[latestReplicaMsgId] reflect the latest state.
     */
    suspend fun sync() {
        suspendCancellableCoroutine { cont ->
            channel.trySend(NewWatcher(SyncWatcher(cont)))
                .onClosed { cont.cancel() }
        }
    }

    override fun toString() =
        "(Watchers {txWatchers=${txWatchers.size}, sourceWatchers=${sourceWatchers.size}, replicaWatchers=${replicaWatchers.size}, " +
                "latestTxId=$latestTxId, latestSourceMsgId=$latestSourceMsgId, latestReplicaMsgId=$latestReplicaMsgId, exception=$exception)})"
}
