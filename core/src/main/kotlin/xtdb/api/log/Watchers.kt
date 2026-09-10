package xtdb.api.log

import kotlinx.coroutines.flow.*
import xtdb.api.TransactionResult
import xtdb.api.TxId
import xtdb.api.tx.ExternalSourceToken
import xtdb.types.MessageId
import xtdb.util.error
import xtdb.util.logger

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

            it.copy(
                latestSourceMsgId = srcMsgId ?: it.latestSourceMsgId,
                latestTxId = txId ?: it.latestTxId,
                latestTxResult = txResult ?: it.latestTxResult,
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

    suspend fun awaitTx(txId: TxId) =
        activeState.first { it.latestTxId >= txId }
            .latestTxResult?.takeIf { it.txKey.txId == txId }

    suspend fun awaitSource(srcMsgId: MessageId) {
        activeState.first { it.latestSourceMsgId >= srcMsgId }
    }

    override fun toString() = state.toString()
}
