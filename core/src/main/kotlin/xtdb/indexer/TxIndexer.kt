package xtdb.indexer

import io.micrometer.core.instrument.Counter
import io.micrometer.tracing.Tracer
import kotlinx.coroutines.CancellationException
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.api.log.Watchers
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSourceToken
import xtdb.indexer.Indexer.Companion.addTxRow
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId

/**
 * Per-tx entry point for external sources.
 * The source provides tx metadata + a [writer] that populates the [OpenTx] and returns a [TxResult]
 * indicating success or failure (with optional `userMetadata` — the source can decide metadata
 * after writing, not before).
 *
 * [indexTx] owns the full lifecycle: smoothing, opening the OpenTx, running the writer,
 * adding the `xt/txs` row, delegating commit to the [TxCommitter], and closing the OpenTx.
 */
class TxIndexer internal constructor(
    private val allocator: BufferAllocator,
    private val nodeBase: NodeBase,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val watchers: Watchers,
    private val committer: TxCommitter,
    private val tracer: Tracer? = null,
    private val instantSource: InstantSource = InstantSource.system(),
) {

    private val dbName get() = dbState.name
    private val liveIndex get() = dbState.liveIndex

    private val txErrorCounter: Counter? =
        nodeBase.meterRegistry?.let { Counter.builder("tx.error").register(it) }

    sealed interface TxResult {
        val userMetadata: Map<*, *>?

        data class Committed(override val userMetadata: Map<*, *>? = null) : TxResult
        data class Aborted(val error: Throwable, override val userMetadata: Map<*, *>? = null) : TxResult
    }

    data class QueryOpts(
        val currentTime: Instant? = null,
        val defaultTz: ZoneId? = null,
    )

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = liveIndex.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    private fun openTx(txKey: TransactionKey, externalSourceToken: ExternalSourceToken?) =
        OpenTx(allocator, nodeBase, dbStorage, dbState, txKey, externalSourceToken, tracer)

    fun startTx(txKey: TransactionKey): OpenTx = openTx(txKey, null)


    suspend fun indexTx(
        externalSourceToken: ExternalSourceToken?,
        txId: Long = (liveIndex.latestCompletedTx?.txId ?: -1) + 1,
        systemTime: Instant = instantSource.instant(),
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult {
        val txKey = TransactionKey(txId, smoothSystemTime(systemTime))

        try {
            val openTx = openTx(txKey, externalSourceToken)
            val result = try {
                writer(openTx)
            } catch (e: Throwable) {
                openTx.close()
                throw e
            }

            when (result) {
                is TxResult.Committed -> openTx.use {
                    it.addTxRow(dbName, txKey, null, result.userMetadata)
                    committer.commit(it, result)
                }

                is TxResult.Aborted -> {
                    txErrorCounter?.increment()
                    openTx.close()
                    openTx(txKey, externalSourceToken).use { abortTx ->
                        abortTx.addTxRow(dbName, txKey, result.error, result.userMetadata)
                        committer.commit(abortTx, result)
                    }
                }
            }

            return result
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            watchers.notifyError(e)
            throw e
        }
    }
}
