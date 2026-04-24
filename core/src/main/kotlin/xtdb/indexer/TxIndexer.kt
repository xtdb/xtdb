package xtdb.indexer

import xtdb.api.TransactionKey
import xtdb.database.ExternalSourceToken
import java.time.Instant

/**
 * Per-tx entry point for external sources.
 * The source provides tx metadata + a [writer] that populates the [OpenTx] and returns a [TxResult]
 * indicating success or failure (with optional `userMetadata` — the source can decide metadata
 * after writing, not before).
 *
 * [indexTx] owns the full lifecycle: smoothing, opening the OpenTx, running the writer,
 * adding the `xt/txs` row, publishing the resulting tx, and closing the OpenTx.
 */
interface TxIndexer {

    sealed interface TxResult {
        val userMetadata: Map<*, *>?

        data class Committed(override val userMetadata: Map<*, *>? = null) : TxResult
        data class Aborted(val error: Throwable, override val userMetadata: Map<*, *>? = null) : TxResult
    }

    fun startTx(txKey: TransactionKey): OpenTx

    suspend fun indexTx(
        externalSourceToken: ExternalSourceToken?,
        txId: Long? = null,
        systemTime: Instant? = null,
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult
}
