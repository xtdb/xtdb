package xtdb.database

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.proto.DatabaseConfig
import java.util.*
import com.google.protobuf.Any as ProtoAny

interface ExternalLog<M> : AutoCloseable {

    fun tailAll(processor: Log.RecordProcessor<M>): Log.Subscription

    interface Factory {
        fun writeTo(dbConfig: DatabaseConfig.Builder)
        fun open(clusters: Map<LogClusterAlias, Log.Cluster>): ExternalLog<*>

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
                if (!dbConfig.hasExternalLog()) return null
                val any = dbConfig.externalLog
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
