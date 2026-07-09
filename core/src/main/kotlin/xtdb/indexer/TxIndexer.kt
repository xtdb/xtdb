package xtdb.indexer

import kotlinx.coroutines.Deferred
import xtdb.database.ExternalSourceToken
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
     * or aborts per the returned [TxResult].
     *
     * [externalSourceToken] is persisted with the tx so the source can resume after it. [systemTime]
     * default to the next monotonic time.
     */
    suspend fun executeTx(
        externalSourceToken: ExternalSourceToken?,
        systemTime: Instant? = null,
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult

    /**
     * Like [executeTx], but hands the transaction off and returns as soon as it is queued — for a high-volume
     * source that only needs ingestion to stay healthy, not each individual result, so it can keep submitting
     * while the indexer works through the backlog. Ordering is preserved by the hand-off queue, so a caller can
     * submit ahead without its transactions being reordered.
     *
     * Returns a durability handle that completes once the transaction is durable; [executeTx] is exactly this
     * followed by awaiting the handle. A fire-and-forget caller ignores it.
     *
     * The hand-off buffer is bounded, so [submitTx] still suspends under backpressure; it just doesn't block on
     * durability. An unrecoverable ingestion failure surfaces on a subsequent [submitTx] or [executeTx] — the call
     * throws the failure cause — so a caller can't keep submitting into a dead indexer and have its transactions
     * silently vanish.
     */
    suspend fun submitTx(
        externalSourceToken: ExternalSourceToken?,
        systemTime: Instant? = null,
        writer: suspend (OpenTx) -> TxResult,
    ): Deferred<Unit>
}
