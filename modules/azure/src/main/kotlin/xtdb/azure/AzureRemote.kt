package xtdb.azure

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote

class AzureRemote(
    val connectionString: String?,
    val storageAccountKey: String?,
) : Remote {
    override fun close() = Unit

    @Serializable
    @SerialName("!Azure")
    data class Factory(
        val connectionString: String? = null,
        val storageAccountKey: String? = null,
    ) : Remote.Factory<AzureRemote> {
        override fun open() = AzureRemote(connectionString, storageAccountKey)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
