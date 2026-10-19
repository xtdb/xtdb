package xtdb.api.log

import kotlinx.coroutines.flow.*
import xtdb.api.TransactionResult
import xtdb.api.TxId
import xtdb.api.tx.ExternalSourceToken
import xtdb.types.MessageId
import xtdb.util.error
import xtdb.util.logger

private val LOG = Watchers::class.logger

private const val RECENT_TXS = 64

private data class AppliedTx(val submittedAt: MessageId?, val result: TransactionResult)

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
        val recentTxs: List<AppliedTx>,
        override val externalSourceToken: ExternalSourceToken? = null,
    ) : State {
        fun sourceTxResult(srcMsgId: MessageId): TransactionResult? =
            recentTxs.lastOrNull { it.submittedAt == srcMsgId }?.result

        fun txResult(txId: TxId): TransactionResult? =
            recentTxs.lastOrNull { it.result.txKey.txId == txId }?.result
    }

    private data class Failed(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        override val externalSourceToken: ExternalSourceToken?,
        val exception: IngestionStoppedException,
    ) : State

    private val state =
        MutableStateFlow<State>(Active(latestSourceMsgId, latestTxId, emptyList(), externalSourceToken))

    private fun State.activeOrThrow(): Active = when (this) {
        is Active -> this
        is Failed -> throw exception
    }

    private val activeState: Flow<Active> get() = state.map { it.activeOrThrow() }

    private inline fun MutableStateFlow<State>.updateIfActive(block: (Active) -> State) {
        update {
            when (it) {
                is Active -> block(it)
                is Failed -> it
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

        state.updateIfActive {
            if (txId != null) check(txId > it.latestTxId) { "txId $txId <= latestTxId ${it.latestTxId}" }
            // >= not >: the BlockUploaded closing a block can carry the same source msgId as the
            // preceding ResolvedTx, when the block was cut by the row gauge with no FlushBlock in between
            if (srcMsgId != null) check(srcMsgId >= it.latestSourceMsgId) {
                "srcMsgId $srcMsgId < latestSourceMsgId ${it.latestSourceMsgId}"
            }

            // an external-source tx carries the source position it resolved at without moving it,
            // so only the tx submitted at a position moves the watermark onto it
            val latestSourceMsgId = it.latestSourceMsgId
            val appliedTx = txResult?.let { res -> AppliedTx(srcMsgId?.takeIf { pos -> pos > latestSourceMsgId }, res) }

            it.copy(
                latestSourceMsgId = srcMsgId ?: it.latestSourceMsgId,
                latestTxId = txId ?: it.latestTxId,
                recentTxs = appliedTx?.let { tx -> (it.recentTxs + tx).takeLast(RECENT_TXS) } ?: it.recentTxs,
                externalSourceToken = extSourceToken ?: it.externalSourceToken,
            )
        }
    }

    fun notifyError(exception: Throwable) {
        state.updateIfActive {
            LOG.error(exception) { "ingestion stopping" }
            Failed(
                latestSourceMsgId = it.latestSourceMsgId,
                latestTxId = it.latestTxId,
                externalSourceToken = it.externalSourceToken,
                exception = exception as? IngestionStoppedException ?: IngestionStoppedException(null, exception),
            )
        }
    }

    /**
     * Returns the result of the transaction [txId] once it has been applied, or null if the watermark passes
     * [txId] without it - a tx-id no transaction took, or a result no longer among the most recent few.
     */
    suspend fun awaitTx(txId: TxId): TransactionResult? =
        activeState.first { it.latestTxId >= txId }.txResult(txId)

    /**
     * Returns the result of the transaction submitted at source position [srcMsgId] once it has been applied,
     * or null if that position carried no transaction, or its result is no longer among the most recent few.
     */
    suspend fun awaitSourceTx(srcMsgId: MessageId): TransactionResult? =
        activeState.first { it.latestSourceMsgId >= srcMsgId }.sourceTxResult(srcMsgId)

    suspend fun awaitSource(srcMsgId: MessageId) {
        activeState.first { it.latestSourceMsgId >= srcMsgId }
    }

    override fun toString() = state.toString()
}
