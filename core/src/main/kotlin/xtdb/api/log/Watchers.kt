package xtdb.api.log

import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await
import xtdb.api.TransactionResult
import xtdb.api.TxId
import xtdb.api.tx.ExternalSourceToken
import xtdb.types.MessageId
import xtdb.util.error
import xtdb.util.logger
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentSkipListMap

private val LOG = Watchers::class.logger

/** @suppress */
class Watchers(
    latestTxId: TxId,
    latestSourceMsgId: MessageId,
    externalSourceToken: ExternalSourceToken? = null,
) {

    private sealed interface State {
        val latestSourceMsgId: MessageId
        val latestTxId: TxId
        val externalSourceToken: ExternalSourceToken?
    }

    private data class Active(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        val latestTxResult: TransactionResult?,
        override val externalSourceToken: ExternalSourceToken? = null,
    ) : State

    private data class Failed(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        override val externalSourceToken: ExternalSourceToken?,
        val exception: IngestionStoppedException,
    ) : State

    private val state =
        MutableStateFlow<State>(Active(latestSourceMsgId, latestTxId, null, externalSourceToken))

    private fun State.activeOrThrow(): Active = when (this) {
        is Active -> this
        is Failed -> throw exception
    }

    private val activeState: Flow<Active> get() = state.map { it.activeOrThrow() }

    private inline fun MutableStateFlow<State>.updateIfActive(block: (Active) -> State): State =
        updateAndGet {
            when (it) {
                is Active -> block(it)
                is Failed -> it
            }
        }

    // A waiter registers before reading the state, and a notify swaps the state before scanning the waiters,
    // so at least one of the two sees the other and the waiter can't be missed.
    private val txWaiters = ConcurrentSkipListMap<TxId, CompletableFuture<TransactionResult?>>()

    private fun releaseTxWaiters(state: State) {
        val released = when (state) {
            is Active -> txWaiters.headMap(state.latestTxId, true)
            is Failed -> txWaiters
        }

        for ((txId, waiter) in released) {
            if (!released.remove(txId, waiter)) continue
            when (state) {
                is Active -> waiter.complete(state.latestTxResult?.takeIf { it.txKey.txId == txId })
                is Failed -> waiter.completeExceptionally(state.exception)
            }
        }
    }

    val latestSourceMsgId: MessageId get() = state.value.latestSourceMsgId
    val latestTxId: TxId get() = state.value.latestTxId
    val externalSourceToken: ExternalSourceToken? get() = state.value.externalSourceToken

    val exception
        get() = when (val v = state.value) {
            is Active -> null
            is Failed -> v.exception
        }

    // --- notify methods ---

    /**
     * One replica record applied, moving every watermark its contents imply in one update — so nothing
     * observes a transaction committed at a source position that has not reached the record carrying it.
     */
    fun notifyApplied(
        srcMsgId: MessageId?,
        txResult: TransactionResult? = null,
        extSourceToken: ExternalSourceToken? = null,
    ) {
        val txId = txResult?.txKey?.txId

        val applied = state.updateIfActive {
            if (txId != null) check(txId > it.latestTxId) { "txId $txId <= latestTxId ${it.latestTxId}" }
            // >= not >: the BlockUploaded closing a block can carry the same source msgId as the
            // preceding ResolvedTx, when the block was cut by the row gauge with no FlushBlock in between
            if (srcMsgId != null) check(srcMsgId >= it.latestSourceMsgId) {
                "srcMsgId $srcMsgId < latestSourceMsgId ${it.latestSourceMsgId}"
            }

            it.copy(
                latestSourceMsgId = srcMsgId ?: it.latestSourceMsgId,
                latestTxId = txId ?: it.latestTxId,
                latestTxResult = txResult ?: it.latestTxResult,
                externalSourceToken = extSourceToken ?: it.externalSourceToken,
            )
        }

        if (txId != null) releaseTxWaiters(applied)
    }

    fun notifyError(exception: Throwable) {
        val failed = state.updateIfActive {
            LOG.error(exception) { "ingestion stopping" }
            Failed(
                latestSourceMsgId = it.latestSourceMsgId,
                latestTxId = it.latestTxId,
                externalSourceToken = it.externalSourceToken,
                exception = exception as? IngestionStoppedException ?: IngestionStoppedException(null, exception),
            )
        }

        releaseTxWaiters(failed)
    }

    /**
     * Completes with the result of the transaction [txId] once it has been applied, or with null if the
     * watermark passes [txId] without its result being the latest - a non-transaction position, or a result
     * already superseded by the time the caller asked.
     * Fails with [IngestionStoppedException] if ingestion stops first.
     *
     * Cancelling the returned future doesn't affect other callers awaiting the same transaction.
     */
    fun awaitTxAsync(txId: TxId): CompletableFuture<TransactionResult?> {
        val waiter = txWaiters.computeIfAbsent(txId) { CompletableFuture() }
        releaseTxWaiters(state.value)
        return waiter.copy()
    }

    suspend fun awaitTx(txId: TxId): TransactionResult? = awaitTxAsync(txId).await()

    suspend fun awaitSource(srcMsgId: MessageId) {
        activeState.first { it.latestSourceMsgId >= srcMsgId }
    }

    override fun toString() = state.toString()
}
