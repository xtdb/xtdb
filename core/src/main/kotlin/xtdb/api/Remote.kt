package xtdb.api

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import java.util.ServiceLoader

typealias RemoteAlias = String

/**
 * A named, node-local handle for an external system XT authenticates against
 * — a Postgres instance, a cloud identity (AWS/Azure/GCP), etc.
 *
 * Credentials and connection details live on the node config, never on the
 * source log. ATTACH payloads carry only the [RemoteAlias]; each node
 * resolves the alias against its own remotes map.
 */
interface Remote : AutoCloseable {

    interface Factory<R : Remote> {
        fun open(): R

        companion object {
            private val registrations = ServiceLoader.load(Registration::class.java).toList()

            val serializersModule = SerializersModule {
                for (reg in registrations) include(reg.serializersModule)

                polymorphic(Factory::class) {
                    for (reg in registrations) reg.registerSerde(this)
                }
            }
        }
    }

    interface Registration {
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory<*>>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
