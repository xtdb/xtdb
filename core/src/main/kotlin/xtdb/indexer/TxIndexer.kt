package xtdb.indexer

import clojure.lang.Keyword
import kotlinx.coroutines.CancellationException
import org.apache.arrow.memory.BufferAllocator
import xtdb.ResultCursor
import xtdb.api.TransactionKey
import xtdb.api.log.Watchers
import xtdb.arrow.RelationReader
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.database.ExternalSourceToken
import xtdb.indexer.Indexer.Companion.addTxRow
import xtdb.query.IQuerySource
import xtdb.table.TableRef
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
 * [indexTx] owns the full lifecycle: smoothing, opening the raw OpenTx, running the writer,
 * adding the `xt/txs` row, delegating commit to the [TxCommitter], and closing the OpenTx.
 */
class TxIndexer internal constructor(
    private val allocator: BufferAllocator,
    private val dbStorage: DatabaseStorage,
    private val dbState: DatabaseState,
    private val querySource: IQuerySource,
    private val watchers: Watchers,
    private val committer: TxCommitter,
    private val instantSource: InstantSource = InstantSource.system(),
) {

    private val dbName get() = dbState.name
    private val liveIndex get() = dbState.liveIndex

    sealed interface TxResult {
        val userMetadata: Map<*, *>?
        data class Committed(override val userMetadata: Map<*, *>? = null) : TxResult
        data class Aborted(val error: Throwable, override val userMetadata: Map<*, *>? = null) : TxResult
    }

    data class QueryOpts(
        val currentTime: Instant? = null,
        val defaultTz: ZoneId? = null,
    )

    /**
     * The per-tx handle passed to the writer lambda in [TxIndexer.indexTx].
     * Writes go via [table]; reads via [openQuery], with visibility into writes made earlier
     * in the same writer call.
     *
     * Scoped to the writer's database — and, once XTDB supports multi-partition external sources,
     * to the current partition. Other databases and partitions are not visible.
     */
    interface OpenTx {

        val systemFrom: Long
        fun table(ref: TableRef): xtdb.indexer.OpenTx.Table

        fun table(schemaName: String, tableName: String): xtdb.indexer.OpenTx.Table

        /**
         * Run SQL against the database's live index, with visibility into writes made earlier
         * in this transaction.
         *
         * Positional `?` parameters are supplied through [args] as a single-row [xtdb.arrow.RelationReader].
         * [xtdb.arrow.Relation.openFromRows] wraps a `List<Map<*, *>>` — keys can be column names
         * or positional placeholders (`_0`, `_1`, …):
         *
         *     Relation.openFromRows(al, listOf(mapOf("_0" to pk))).use { rel ->
         *         openTx.openQuery("SELECT * FROM t WHERE _id = ?", rel.relReader).use { cursor ->
         *             …
         *         }
         *     }
         *
         * Defaults for [opts] are derived from the tx's system-time and the database's default
         * timezone; most callers can omit it.
         */
        fun openQuery(sql: String, args: RelationReader? = null, opts: QueryOpts = QueryOpts()): ResultCursor
    }

    private fun smoothSystemTime(systemTime: Instant): Instant {
        val lct = liveIndex.latestCompletedTx?.systemTime ?: return systemTime
        val floor = fromMicros(lct.asMicros + 1)
        return if (systemTime.isBefore(floor)) floor else systemTime
    }

    private inner class WriterOpenTx(val inner: xtdb.indexer.OpenTx) : OpenTx {
        override val systemFrom get() = inner.systemFrom

        override fun table(ref: TableRef) = inner.table(ref)
        override fun table(schemaName: String, tableName: String) =
            inner.table(TableRef(dbName, schemaName, tableName))

        override fun openQuery(sql: String, args: RelationReader?, opts: QueryOpts): ResultCursor {
            val currentTime = opts.currentTime ?: inner.txKey.systemTime

            val snapSource = object : Snapshot.Source {
                override fun openSnapshot() = liveIndex.openSnapshot(inner)
            }
            val queryCat = Indexer.queryCatalog(dbStorage, dbState, snapSource)

            val prepareOpts = mapOf(
                Keyword.intern("current-time") to currentTime,
                Keyword.intern("default-tz") to opts.defaultTz,
                Keyword.intern("default-db") to dbName,
                Keyword.intern("query-text") to sql,
            )

            val pq = querySource.prepareQuery(sql, queryCat, prepareOpts)
            return pq.openQuery(args, xtdb.query.QueryOpts(currentTime = currentTime, defaultTz = opts.defaultTz))
        }
    }

    suspend fun indexTx(
        externalSourceToken: ExternalSourceToken?,
        systemTime: Instant = instantSource.instant(),
        writer: suspend (OpenTx) -> TxResult,
    ): TxResult {
        val txId = (liveIndex.latestCompletedTx?.txId ?: -1) + 1
        val txKey = TransactionKey(txId, smoothSystemTime(systemTime))

        try {
            val openTx = OpenTx(allocator, txKey, externalSourceToken)
            val result = try {
                writer(WriterOpenTx(openTx))
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
                    openTx.close()
                    OpenTx(allocator, txKey, externalSourceToken).use { abortTx ->
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
