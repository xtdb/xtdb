package xtdb.database

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.error.Unsupported
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
        fun open(
            dbName: String,
            remotes: Map<RemoteAlias, Remote>,
            meterRegistry: MeterRegistry? = null,
        ): ExternalSource

        companion object {
            private val registrations = ServiceLoader.load(Registration::class.java).toList()
            private val registrationsByTag = registrations.associateBy { it.protoTag }
            private val registrationsByClass = registrations.associateBy { it.factoryClass }

            val serializersModule = SerializersModule {
                for (reg in registrations)
                    include(reg.serializersModule)

                polymorphic(Factory::class) {
                    for (reg in registrations)
                        reg.registerSerde(this)
                }
            }

            fun fromProto(any: ProtoAny): Factory {
                val reg = registrationsByTag[any.typeUrl] ?: error("unknown external source: ${any.typeUrl}")
                return reg.fromProto(any)
            }

            // A factory is persistable iff a Registration claims its class. A programmatically-supplied
            // source with no Registration is a legal Factory, but can't travel as a serialised secondary.
            @Suppress("UNCHECKED_CAST")
            fun toProto(factory: Factory): ProtoAny {
                val reg = registrationsByClass[factory.javaClass] as Registration<Factory>?
                    ?: throw Unsupported(
                        "external source ${factory.javaClass.name} can't be persisted as a secondary database — " +
                            "it has no Registration",
                        "xtdb/external-source-not-serializable",
                        mapOf("external-source" to factory.javaClass.name),
                    )
                return reg.toProto(factory)
            }
        }
    }

    interface Registration<F : Factory> {
        val protoTag: String
        val factoryClass: Class<F>
        fun toProto(factory: F): ProtoAny
        fun fromProto(msg: ProtoAny): F
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
