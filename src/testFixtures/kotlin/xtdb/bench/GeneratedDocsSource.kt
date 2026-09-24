package xtdb.bench

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.runBlocking
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.TransactionResult
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult

/**
 * An external source that puts `_id`s `0 until docCount` into [tableName], [batchSize] documents per
 * transaction, handing each off with [TxIndexer.submitTx] rather than awaiting it.
 *
 * It waits for [ingest] before submitting anything, so a caller can time the ingest apart from opening
 * the database.
 */
class GeneratedDocsSource(
    private val tableName: String,
    private val docCount: Long,
    private val batchSize: Int,
) : ExternalSource.Factory {

    private val started = CompletableDeferred<Unit>()
    private val ingested = CompletableDeferred<TransactionResult>()

    /**
     * Starts the ingest and blocks until every transaction is replicated and applied.
     *
     * @return the last transaction's result
     */
    fun ingest(): TransactionResult = runBlocking {
        started.complete(Unit)
        ingested.await()
    }

    override fun open(dbName: String, remotes: Map<RemoteAlias, Remote>, meterRegistry: MeterRegistry?) =
        object : ExternalSource {
            override suspend fun onPartitionAssigned(
                partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
            ) {
                started.await()

                try {
                    var last: Deferred<TransactionResult>? = null

                    for (batchStart in 0 until docCount step batchSize.toLong()) {
                        val batchEnd = minOf(batchStart + batchSize, docCount)

                        last = txIndexer.submitTx(null) { openTx ->
                            val table = openTx.table(tableName = tableName)
                            for (id in batchStart until batchEnd) table.writePut(mapOf("_id" to id))
                            TxResult.Committed()
                        }
                    }

                    ingested.complete(checkNotNull(last) { "docCount must be positive" }.await())
                } catch (e: Throwable) {
                    ingested.completeExceptionally(e)
                    throw e
                }
            }

            override fun close() = Unit
        }
}
