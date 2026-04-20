package xtdb.database

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.proto.DatabaseConfig
import xtdb.indexer.OpenTx
import java.time.Instant
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
