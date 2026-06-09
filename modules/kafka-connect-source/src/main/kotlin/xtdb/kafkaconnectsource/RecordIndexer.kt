package xtdb.kafkaconnectsource

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.apache.kafka.connect.sink.SinkRecord
import xtdb.indexer.TxIndexer
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

interface RecordIndexer : AutoCloseable {

    suspend fun indexRecords(records: List<SinkRecord>, txIndexer: TxIndexer)

    override fun close() = Unit

    interface Factory {
        fun toProto(): ProtoAny
        fun open(dbName: String): RecordIndexer

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

            fun fromProto(any: ProtoAny): Factory {
                val reg = registrationsByTag[any.typeUrl]
                    ?: error("unknown record indexer: ${any.typeUrl}")
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
