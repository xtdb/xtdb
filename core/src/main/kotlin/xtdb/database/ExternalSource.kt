package xtdb.database

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.ResultCursor
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.arrow.RelationReader
import xtdb.database.proto.DatabaseConfig
import xtdb.indexer.OpenTx.Table
import xtdb.table.TableRef
import java.time.Instant
import java.time.ZoneId
import java.util.*
import com.google.protobuf.Any as ProtoAny

typealias ExternalSourceToken = ProtoAny

interface ExternalSource : AutoCloseable {

    suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer)

    /**
     * Per-tx entry point for external sources.
     * The source provides tx metadata + a [writer] that populates the [OpenTx] and returns a [TxResult]
     * indicating success or failure (with optional `userMetadata` — the source can decide metadata
     * after writing, not before).
     *
     * `indexTx` owns the full lifecycle: smoothing, opening the raw OpenTx, running the writer,
     * adding the `xt/txs` row, committing (or aborting) the live index, appending to the replica log,
     * and closing the OpenTx.
     */
    sealed interface TxResult {
        val userMetadata: Map<*, *>?
        data class Committed(override val userMetadata: Map<*, *>? = null) : TxResult
        data class Aborted(val error: Throwable, override val userMetadata: Map<*, *>? = null) : TxResult
    }

    interface TxIndexer {
        suspend fun indexTx(
            externalSourceToken: ExternalSourceToken?,
            systemTime: Instant? = null,
            writer: suspend (OpenTx) -> TxResult,
        ): TxResult
    }

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

        fun table(ref: TableRef): Table
        fun table(schemaName: String, tableName: String): Table

        /**
         * Run SQL against the database's live index, with visibility into writes made earlier
         * in this transaction.
         *
         * Positional `?` parameters are supplied through [args] as a single-row [RelationReader].
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

    data class QueryOpts(
        val currentTime: Instant? = null,
        val defaultTz: ZoneId? = null,
    )

    interface Factory {
        fun writeTo(dbConfig: DatabaseConfig.Builder)
        fun open(
            dbName: String,
            clusters: Map<LogClusterAlias, Log.Cluster>,
            remotes: Map<RemoteAlias, Remote>,
        ): ExternalSource

        companion object {
            private val registrations = ServiceLoader.load(Registration::class.java).toList()
            private val registrationsByTag = registrations.associateBy { it.protoTag }

            val serializersModule = SerializersModule {
                for (reg in registrations)
                    include(reg.serializersModule)

                polymorphic(Factory::class) {
                    for (reg in registrations)
                        reg.registerSerde(this)
                }
            }

            fun fromProto(dbConfig: DatabaseConfig): Factory? {
                if (!dbConfig.hasExternalSource()) return null
                val any = dbConfig.externalSource
                val reg = registrationsByTag[any.typeUrl] ?: error("unknown external source: ${any.typeUrl}")
                return reg.fromProto(any)
            }
        }
    }

    interface Registration {
        val protoTag: String
        fun fromProto(msg: ProtoAny): Factory
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
