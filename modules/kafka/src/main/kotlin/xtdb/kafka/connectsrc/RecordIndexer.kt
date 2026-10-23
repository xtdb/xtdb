package xtdb.kafka.connectsrc

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.apache.kafka.connect.sink.SinkRecord
import xtdb.api.error.Unsupported
import xtdb.api.tx.TxIndexer
import xtdb.kafka.connectsrc.proto.KafkaConnectSourceToken
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

interface RecordIndexer : AutoCloseable {

    suspend fun indexRecords(records: List<SinkRecord>, txIndexer: TxIndexer)

    override fun close() = Unit

    interface Factory {
        fun open(): RecordIndexer

        /**
         * Opens the indexer for a leader term, handing back the state it persisted.
         *
         * @param userToken the `user_token` of the last indexed transaction's [KafkaConnectSourceToken],
         *   or null if no transaction has been indexed or that token carried none.
         *   An indexer that keeps state across terms writes it there when it submits a transaction.
         */
        fun open(userToken: ByteArray?): RecordIndexer = open()

        companion object {
            // Lazy: a Factory implementation's class init can initialise this interface, and loading
            // the Registrations then re-enters that half-initialised implementation.
            private val registrations by lazy { ServiceLoader.load(Registration::class.java).toList() }
            private val registrationsByTag by lazy { registrations.associateBy { it.protoTag } }
            private val registrationsByClass by lazy { registrations.associateBy { it.factoryClass } }

            val serializersModule: SerializersModule by lazy {
                SerializersModule {
                    for (reg in registrations)
                        include(reg.serializersModule)

                    polymorphic(Factory::class) {
                        for (reg in registrations)
                            reg.registerSerde(this)
                    }
                }
            }

            fun fromProto(any: ProtoAny): Factory {
                val reg = registrationsByTag[any.typeUrl]
                    ?: error("unknown record indexer: ${any.typeUrl}")
                return reg.fromProto(any)
            }

            // A factory is persistable iff a Registration claims its class. A programmatically-supplied
            // indexer with no Registration is a legal Factory, but can't travel as a serialised secondary.
            @Suppress("UNCHECKED_CAST")
            fun toProto(factory: Factory): ProtoAny {
                val reg = registrationsByClass[factory.javaClass] as Registration<Factory>?
                    ?: throw Unsupported(
                        "record indexer ${factory.javaClass.name} can't be persisted as a secondary database — " +
                            "it has no Registration",
                        "xtdb.kafka-connect-source/indexer-not-serializable",
                        mapOf("indexer" to factory.javaClass.name),
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
