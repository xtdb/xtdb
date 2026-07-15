package xtdb.api.tx

import kotlinx.coroutines.Deferred
import xtdb.api.TransactionResult
import java.time.Instant

/**
 * Per-tx entry point for external sources.
 * The source provides tx metadata + a [writer] that populates the [OpenTx] and returns a [TxResult]
 * indicating success or failure (with optional `userMetadata` — the source can decide metadata
 * after writing, not before).
 *
 * [executeTx] and [submitTx] own the full lifecycle: smoothing, opening the OpenTx, running the writer,
 * adding the `xt/txs` row, publishing the resulting tx, and closing the OpenTx. They differ only in
 * whether the caller waits for the result.
 */
interface TxIndexer {

    /** The outcome a [writer] returns: commit or abort, plus optional `userMetadata` recorded on the `xt/txs` table. */
    sealed interface TxResult {
        val userMetadata: Map<*, *>?

        data class Committed(override val userMetadata: Map<*, *>? = null) : TxResult
        data class Aborted(val error: Throwable, override val userMetadata: Map<*, *>? = null) : TxResult
    }

    /**
     * Indexes one external-source transaction: opens an [OpenTx], runs [writer] to populate it, then commits
     * or aborts per the returned [TxResult]. Returns only once the tx is durably replicated.
     *
     * [externalSourceToken] is persisted with the tx so the source can resume after it. [systemTime]
     * defaults to the next monotonic time.
     *
     * @return the durably-replicated outcome, carrying the [xtdb.api.TransactionKey] assigned to the tx.
     */
    suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?,
        systemTime: Instant? = null,
        writer: suspend (OpenTx) -> TxResult,
    ): TransactionResult

    /**
     * Like [executeTx], but hands the transaction off and returns a durability handle without waiting on it — for a
     * high-volume source that keeps submitting while the indexer works through the backlog, acknowledging progress
     * out of band once transactions become durable (e.g. the Postgres source advancing its replication-slot LSN).
     *
     * Transactions are sequenced in submission order: the hand-off onto the ordering queue completes before [submitTx]
     * returns, so sequential calls can't be reordered — the tx from the first call is sequenced ahead of the second's.
     * (Calls raced concurrently from different coroutines are ordered by whichever wins the hand-off, as you'd expect.)
     * The returned [Deferred] completes with the tx's [TransactionResult] once it's durably replicated, in that same
     * order.
     *
     * A caller that doesn't need per-tx results may discard the handle: the hand-off buffer is bounded (so [submitTx]
     * still suspends under backpressure), and an unrecoverable ingestion failure also surfaces on a subsequent
     * [submitTx]/[executeTx] — the call throws the failure cause — so transactions can't silently vanish into a dead
     * indexer.
     */
    suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?,
        systemTime: Instant? = null,
        writer: suspend (OpenTx) -> TxResult,
    ): Deferred<TransactionResult>
}
