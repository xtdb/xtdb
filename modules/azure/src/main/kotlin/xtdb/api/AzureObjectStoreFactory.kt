@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)
package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import xtdb.util.requiringResolve
import java.nio.file.Path

@Serializable
@SerialName("!Azure")
data class AzureObjectStoreFactory @JvmOverloads constructor(
    val storageAccount: String,
    val container: String,
    val servicebusNamespace: String,
    val servicebusTopicName: String,
    var prefix: Path? = null,
) : ObjectStoreFactory {
    companion object {
        private val OPEN_OBJECT_STORE = requiringResolve("xtdb.azure", "open-object-store")
    }

    fun prefix(prefix: Path) = apply { this.prefix = prefix }
    override fun openObjectStore() = OPEN_OBJECT_STORE.invoke(this) as ObjectStore

    class Registration: ModuleRegistration {
        override fun register(registry: ModuleRegistry) {
            registry.registerObjectStore(AzureObjectStoreFactory::class)
        }
    }
}
