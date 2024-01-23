@file:UseSerializers(PathWithEnvVarSerde::class, StringWithEnvVarSerde::class)
package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.Transient
import xtdb.util.requiringResolve
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import xtdb.s3.S3Configurator
import java.nio.file.Path

data object DefaultS3Configurator: S3Configurator

@Serializable
@SerialName("!S3")
data class S3ObjectStoreFactory @JvmOverloads constructor(
    val bucket: String,
    val snsTopicArn: String,
    var prefix: Path? = null,
    @Transient var s3Configurator: S3Configurator = DefaultS3Configurator
) : ObjectStoreFactory {
    companion object {
        private val OPEN_OBJECT_STORE = requiringResolve("xtdb.s3", "open-object-store")
    }

    fun prefix(prefix: Path) = apply { this.prefix = prefix }
    fun s3Configurator(s3Configurator: S3Configurator) = apply { this.s3Configurator = s3Configurator }

    override fun openObjectStore() = OPEN_OBJECT_STORE.invoke(this) as ObjectStore

    class Registration: ModuleRegistration {
        override fun register(registry: ModuleRegistry) {
            registry.registerObjectStore(S3ObjectStoreFactory::class)
        }
    }
}
