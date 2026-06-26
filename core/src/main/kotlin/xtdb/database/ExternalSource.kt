package xtdb.database

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.error.Unsupported
import xtdb.indexer.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import java.util.*
import com.google.protobuf.Any as ProtoAny

/** Opaque resume marker (e.g. a serialised log offset) that XTDB persists per tx and hands back on resume. */
typealias ExternalSourceToken = ByteArray

/**
 * SPI for streaming transactions into a database from an upstream feed (a CDC source, message log, etc.)
 * instead of XTDB's own DML.
 *
 * The source owns the read side of its upstream: it receives events (via polling or other methods),
 * maps each to a transaction, and submits it through [TxIndexer.executeTx].
 *
 * XTDB drives the source via [onPartitionAssigned] and persists the per-tx [ExternalSourceToken]
 * so the source can resume after the last indexed event.
 */
interface ExternalSource : AutoCloseable {

    /**
     * Called when [partition] is assigned to this node, to run the source's poll loop.
     *
     * Resume from just after [afterToken] - the token of the last tx already indexed for this partition, or
     * null to start from the beginning - submitting each upstream event via [txIndexer].
     *
     * Implementors must follow Kotlin's prompt cancellation guarantees - when the coroutine executing this method
     * is cancelled, the implementor must clean up and cede control as soon as possible.
     */
    suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer)

    /**
     * Opens an [ExternalSource] for a database, from the source config.
     *
     * See [Registration] to make an external source implementation available through `ATTACH DATABASE`.
     */
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

    /**
     * Makes a [Factory] persistable. A factory whose class is claimed by a `ServiceLoader`-discovered
     * registration serialises (via [protoTag] / [toProto]) and so can travel as a stored secondary-database
     * config; a programmatic factory with no registration still runs in-process, but [Factory.toProto] rejects it.
     */
    interface Registration<F : Factory> {
        val protoTag: String
        val factoryClass: Class<F>
        fun toProto(factory: F): ProtoAny
        fun fromProto(msg: ProtoAny): F
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
