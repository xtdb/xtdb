@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)
package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.PathSerde
import xtdb.util.requiringResolve
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import java.nio.file.Path

@Serializable
@SerialName("!GoogleCloud")
data class GoogleCloudObjectStoreFactory @JvmOverloads constructor(
    val projectId: String,
    val bucket: String,
    val pubsubTopic: String,
    var prefix: Path? = null,
) : ObjectStoreFactory {
    companion object {
        private val OPEN_OBJECT_STORE = requiringResolve("xtdb.google-cloud", "open-object-store")
    }

    fun prefix(prefix: Path) = apply { this.prefix = prefix }

    override fun openObjectStore() = OPEN_OBJECT_STORE.invoke(this) as ObjectStore

    class Registration: ModuleRegistration {
        override fun register(registry: ModuleRegistry) {
            registry.registerObjectStore(GoogleCloudObjectStoreFactory::class)
        }
    }
}
