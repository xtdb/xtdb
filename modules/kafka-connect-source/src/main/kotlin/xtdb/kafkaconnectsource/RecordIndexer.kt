package xtdb.kafkaconnectsource

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.apache.kafka.connect.sink.SinkRecord
import xtdb.indexer.TxIndexer
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

/**
 * Indexes a batch of [SinkRecord]s into XT transactions.
 *
 * [SinkRecord]s have already been through the configured key/value [org.apache.kafka.connect.storage.Converter]s
 * and any configured [org.apache.kafka.connect.transforms.Transformation] chain by the time they reach an indexer —
 * `value()` is a Connect [org.apache.kafka.connect.data.Struct] (or primitive / `Map` for schemaless converters),
 * with `valueSchema()` describing its shape.
 *
 * The same SPI shape is what Kafka Connect's `SinkTask.put(records)` delivers — by design, so the indexer
 * works identically under [KafkaConnectSource] (XTDB owns the consumer) and under a future KC sink connector
 * (KC's runtime owns the consumer).
 */
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
