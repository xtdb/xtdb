package xtdb.api.log

import kotlinx.coroutines.flow.*
import xtdb.api.TransactionResult
import xtdb.api.TxId
import xtdb.database.ExternalSourceToken

class Watchers(latestTxId: TxId, latestSourceMsgId: MessageId, externalSourceToken: ExternalSourceToken? = null) {

    /**
     * Backward-compat constructor for setups where tx-id is always a src msg-id.
     */
    constructor(latestSourceMsgId: MessageId) : this(latestSourceMsgId, latestSourceMsgId)

    private sealed interface State

    private data class Active(
        val latestSourceMsgId: MessageId, val latestTxId: TxId, val latestTxResult: TransactionResult?,
        val externalSourceToken: ExternalSourceToken? = null,
    ) : State

    private data class Failed(val exception: IngestionStoppedException) : State

    private val state = MutableStateFlow<State>(Active(latestSourceMsgId, latestTxId, null, externalSourceToken))

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

    val latestSourceMsgId get() = state.value.activeOrThrow().latestSourceMsgId
    val externalSourceToken: ExternalSourceToken? get() = state.value.activeOrThrow().externalSourceToken

    val exception
        get() = when (val v = state.value) {
            is Active -> null
            is Failed -> v.exception
        }

    // --- notify methods ---

    fun notifyTx(result: TransactionResult, srcMsgId: MessageId, externalSourceToken: ExternalSourceToken?) {
        val txId = result.txKey.txId
        state.updateIfActive {
            check(txId > it.latestTxId) { "txId $txId <= latestTxId ${it.latestTxId}" }
            // >= not >: BlockBoundary can carry the same source msgId as the preceding ResolvedTx
            // when the block was triggered by isFull() (no FlushBlock in between)
            check(srcMsgId >= it.latestSourceMsgId) { "srcMsgId $srcMsgId < latestSourceMsgId ${it.latestSourceMsgId}" }
            it.copy(
                latestSourceMsgId = srcMsgId, latestTxId = txId, latestTxResult = result,
                externalSourceToken = externalSourceToken ?: it.externalSourceToken,
            )
        }
    }

    fun notifyMsg(srcMsgId: MessageId) {
        state.updateIfActive {
            // >= not >: BlockBoundary can carry the same source msgId as the preceding ResolvedTx
            // when the block was triggered by isFull() (no FlushBlock in between)
            check(srcMsgId >= it.latestSourceMsgId) { "srcMsgId $srcMsgId < latestSourceMsgId ${it.latestSourceMsgId}" }
            it.copy(latestSourceMsgId = srcMsgId)
        }
    }

    fun notifyError(exception: Throwable) {
        state.updateIfActive {
            Failed(exception as? IngestionStoppedException ?: IngestionStoppedException(null, exception))
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
