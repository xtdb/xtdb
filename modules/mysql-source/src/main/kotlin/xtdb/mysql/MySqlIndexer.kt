package xtdb.mysql

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.api.tx.OpenTx
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

/**
 * Writes a source [MySqlDriver.Transaction] into XT via the supplied [OpenTx].
 *
 * The source owns the token, system-time and failure model — the indexer runs *inside* a token-managed [OpenTx]
 * and just decides what to write.
 * One upstream transaction is one call, whatever tables it touched, and one with no rows in the included tables
 * still arrives, with no ops.
 * Snapshot and streaming both arrive as a [MySqlDriver.Transaction]; the snapshot path synthesises one per batch,
 * so there is a single entry point.
 *
 * Each op carries its source `database`/`table` and the table's primary-key columns ([RowOp]), so an indexer can
 * re-route tables, derive `_id` from the key, drop or mask columns, or filter ops entirely.
 */
interface MySqlIndexer : AutoCloseable {

    fun indexTx(tx: MySqlDriver.Transaction, openTx: OpenTx)

    override fun close() = Unit

    interface Factory {
        fun open(): MySqlIndexer

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
                val reg = registrationsByTag[any.typeUrl]
                    ?: throw Incorrect(
                        "unknown MySQL indexer '${any.typeUrl}' — is the module that registers it on the classpath?",
                        "xtdb.mysql/unknown-indexer", mapOf("indexer" to any.typeUrl),
                    )
                return reg.fromProto(any)
            }

            @Suppress("UNCHECKED_CAST")
            fun toProto(factory: Factory): ProtoAny {
                val reg = registrationsByClass[factory.javaClass] as Registration<Factory>?
                    ?: throw Unsupported(
                        "mysql indexer ${factory.javaClass.name} can't be persisted as a secondary database — " +
                            "it has no Registration",
                        "xtdb.mysql/indexer-not-serializable",
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
        fun fromProto(msg: ProtoAny): Factory
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
