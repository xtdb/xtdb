package xtdb.api.log

import kotlinx.coroutines.flow.*
import xtdb.api.TransactionResult
import xtdb.api.error.Interrupted
import xtdb.api.TxId
import xtdb.api.tx.ExternalSourceToken
import xtdb.types.MessageId
import xtdb.util.error
import xtdb.util.logger
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentSkipListMap

private val LOG = Watchers::class.logger

private const val RECENT_SOURCE_TXS = 64

private data class SourceTx(val srcMsgId: MessageId, val result: TransactionResult)

/** @suppress */
class Watchers(
    latestTxId: TxId,
    latestSourceMsgId: MessageId,
    externalSourceToken: ExternalSourceToken? = null,
) : AutoCloseable {

    private sealed interface State {
        val latestSourceMsgId: MessageId
        val latestTxId: TxId
        val externalSourceToken: ExternalSourceToken?
    }

    private data class Active(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        val latestTxResult: TransactionResult?,
        val recentSourceTxs: List<SourceTx>,
        override val externalSourceToken: ExternalSourceToken? = null,
    ) : State {
        fun sourceTxResult(srcMsgId: MessageId): TransactionResult? =
            recentSourceTxs.lastOrNull { it.srcMsgId == srcMsgId }?.result
    }

    private data class Failed(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        override val externalSourceToken: ExternalSourceToken?,
        val exception: IngestionStoppedException,
    ) : State

    private val state =
        MutableStateFlow<State>(Active(latestSourceMsgId, latestTxId, null, emptyList(), externalSourceToken))

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
    private val sourceTxWaiters = ConcurrentSkipListMap<MessageId, CompletableFuture<TransactionResult?>>()

    private fun releaseSourceTxWaiters(state: State) {
        val released = when (state) {
            is Active -> sourceTxWaiters.headMap(state.latestSourceMsgId, true)
            is Failed -> sourceTxWaiters
        }

        for ((srcMsgId, waiter) in released) {
            if (!released.remove(srcMsgId, waiter)) continue
            when (state) {
                is Active -> waiter.complete(state.sourceTxResult(srcMsgId))
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

            // an external-source tx carries the source position it resolved at without moving it,
            // so only the tx submitted at a position moves the watermark onto it
            val sourceTx = if (txResult != null && srcMsgId != null && srcMsgId > it.latestSourceMsgId) SourceTx(srcMsgId, txResult) else null

            it.copy(
                latestSourceMsgId = srcMsgId ?: it.latestSourceMsgId,
                latestTxId = txId ?: it.latestTxId,
                latestTxResult = txResult ?: it.latestTxResult,
                recentSourceTxs = sourceTx?.let { tx -> (it.recentSourceTxs + tx).takeLast(RECENT_SOURCE_TXS) } ?: it.recentSourceTxs,
                externalSourceToken = extSourceToken ?: it.externalSourceToken,
            )
        }

        if (srcMsgId != null) releaseSourceTxWaiters(applied)
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

        releaseSourceTxWaiters(failed)
    }

    /**
     * Completes once the source position [srcMsgId] has been applied, with the result of the transaction submitted
     * there - or with null if that position carried no transaction, or its result is no longer among the
     * most recent few.
     * Fails with [IngestionStoppedException] if ingestion stops first.
     *
     * Cancelling the returned future doesn't affect other callers awaiting the same position.
     */
    fun awaitSourceTxAsync(srcMsgId: MessageId): CompletableFuture<TransactionResult?> {
        val waiter = sourceTxWaiters.computeIfAbsent(srcMsgId) { CompletableFuture() }
        releaseSourceTxWaiters(state.value)
        return waiter.copy()
    }

    /**
     * Returns the result of the transaction [txId] once it has been applied, or null if the watermark passes
     * [txId] without its result being the latest.
     */
    suspend fun awaitTx(txId: TxId): TransactionResult? =
        activeState.first { it.latestTxId >= txId }.latestTxResult?.takeIf { it.txKey.txId == txId }

    /** Fails every transaction still awaited, as the database is going away. */
    override fun close() {
        val closed = Interrupted("database closed", "xtdb/database-closed")
        for ((srcMsgId, waiter) in sourceTxWaiters)
            if (sourceTxWaiters.remove(srcMsgId, waiter)) waiter.completeExceptionally(closed)
    }

    suspend fun awaitSource(srcMsgId: MessageId) {
        activeState.first { it.latestSourceMsgId >= srcMsgId }
    }

    override fun toString() = state.toString()
}
