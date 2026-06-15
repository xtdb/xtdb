package xtdb.database

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.indexer.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.database.proto.DatabaseConfig
import java.util.*
import com.google.protobuf.Any as ProtoAny

typealias ExternalSourceToken = ByteArray

interface ExternalSource : AutoCloseable {

    suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer)

    interface Factory {
        fun toProto(): ProtoAny
        fun open(
            dbName: String,
            remotes: Map<RemoteAlias, Remote>,
            meterRegistry: MeterRegistry? = null,
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
