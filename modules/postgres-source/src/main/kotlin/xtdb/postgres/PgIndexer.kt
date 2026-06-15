package xtdb.postgres

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.error.Unsupported
import xtdb.indexer.OpenTx
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

/**
 * Writes a source [PostgresDriver.Transaction] into XT via the supplied [OpenTx].
 *
 * The source owns the token, system-time and failure model — the indexer runs *inside* a
 * token-managed [OpenTx] and just decides what to write. Snapshot and streaming both arrive as a
 * [PostgresDriver.Transaction]; the snapshot path synthesises one per batch so there is a single
 * entry point.
 *
 * Each op carries its source `schema`/`table` ([RowOp]), so an indexer can re-route tables, derive
 * `_id`, drop or mask columns, or filter ops entirely. The reference [DirectMirror] mirrors the
 * upstream as-is and has no privileged access — it uses the same [OpenTx] surface as any indexer.
 */
interface PgIndexer : AutoCloseable {

    fun indexTx(tx: PostgresDriver.Transaction, openTx: OpenTx)

    override fun close() = Unit

    interface Factory {
        fun open(dbName: String): PgIndexer

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
                    ?: error("unknown pg indexer: ${any.typeUrl}")
                return reg.fromProto(any)
            }

            // A factory is persistable iff a Registration claims its class. A programmatically-supplied
            // indexer with no Registration is a legal Factory, but can't travel as a serialised secondary.
            @Suppress("UNCHECKED_CAST")
            fun toProto(factory: Factory): ProtoAny {
                val reg = registrationsByClass[factory.javaClass] as Registration<Factory>?
                    ?: throw Unsupported(
                        "pg indexer ${factory.javaClass.name} can't be persisted as a secondary database — " +
                            "it has no Registration",
                        "xtdb.postgres/indexer-not-serializable",
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
