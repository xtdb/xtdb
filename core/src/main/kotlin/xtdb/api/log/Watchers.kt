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
    latestReplicaMsgId: MessageId,
    externalSourceToken: ExternalSourceToken? = null,
) {

    private sealed interface State {
        val latestSourceMsgId: MessageId
        val latestTxId: TxId
        val latestReplicaMsgId: MessageId
        val externalSourceToken: ExternalSourceToken?
    }

    private data class Active(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        override val latestReplicaMsgId: MessageId,
        val latestTxResult: TransactionResult?,
        override val externalSourceToken: ExternalSourceToken? = null,
    ) : State

    private data class Failed(
        override val latestSourceMsgId: MessageId,
        override val latestTxId: TxId,
        override val latestReplicaMsgId: MessageId,
        override val externalSourceToken: ExternalSourceToken?,
        val exception: IngestionStoppedException,
    ) : State

    private val state =
        MutableStateFlow<State>(Active(latestSourceMsgId, latestTxId, latestReplicaMsgId, null, externalSourceToken))

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
    val latestReplicaMsgId: MessageId get() = state.value.latestReplicaMsgId
    val externalSourceToken: ExternalSourceToken? get() = state.value.externalSourceToken

    val exception
        get() = when (val v = state.value) {
            is Active -> null
            is Failed -> v.exception
        }

    // --- notify methods ---

    /**
     * One replica record applied, moving every watermark its contents imply in one update — so nothing
     * observes a transaction committed at a consume position that has not reached the record carrying it.
     *
     * [replicaMsgId] is null where the caller has no consume position of its own: a record held during a
     * block and replayed once the block lands, whose position was counted when it was first read, or a
     * term replaying what the outgoing follower had already read.
     *
     * The position is unchecked where the source watermark is checked: it regresses when a term opens at
     * its replay target, below where the outgoing follower had read. The records in between are the
     * superseded leader's, fenced by our own claim sitting before them, so re-reading applies nothing.
     */
    fun notifyApplied(
        replicaMsgId: MessageId?,
        srcMsgId: MessageId? = null,
        txResult: TransactionResult? = null,
        extSourceToken: ExternalSourceToken? = null,
    ) {
        val txId = txResult?.txKey?.txId

        state.updateIfActive {
            if (txId != null) check(txId > it.latestTxId) { "txId $txId <= latestTxId ${it.latestTxId}" }
            // >= not >: BlockBoundary can carry the same source msgId as the preceding ResolvedTx
            // when the block was triggered by isFull() (no FlushBlock in between)
            if (srcMsgId != null) check(srcMsgId >= it.latestSourceMsgId) {
                "srcMsgId $srcMsgId < latestSourceMsgId ${it.latestSourceMsgId}"
            }

            it.copy(
                latestSourceMsgId = srcMsgId ?: it.latestSourceMsgId,
                latestTxId = txId ?: it.latestTxId,
                latestReplicaMsgId = replicaMsgId ?: it.latestReplicaMsgId,
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
                latestReplicaMsgId = it.latestReplicaMsgId,
                externalSourceToken = it.externalSourceToken,
                exception = exception as? IngestionStoppedException ?: IngestionStoppedException(null, exception),
            )
        }
    }

    suspend fun awaitReplicaMsg(msgId: MessageId) {
        activeState.first { it.latestReplicaMsgId >= msgId }
    }

    suspend fun awaitTx(txId: TxId) =
        activeState.first { it.latestTxId >= txId }
            .latestTxResult?.takeIf { it.txKey.txId == txId }

    suspend fun awaitSource(srcMsgId: MessageId) {
        activeState.first { it.latestSourceMsgId >= srcMsgId }
    }

    override fun toString() = state.toString()
}
