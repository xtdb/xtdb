@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)
package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import xtdb.util.requiringResolve
import java.nio.file.Path

/**
 * Used to set configuration options for an Azure Blob Storage Object Store,
 * which can be used as implementation of objectStore within a [xtdb.api.storage.RemoteStorageFactory].
 *
 * Requires at least **storageAccount**, **container**, **servicebusNamespace** and **servicebusTopicName** to be provided - these will need to be accessible to whichever
 * authentication credentials you use. Authentication for the components in the module is done via the
 * [DefaultAzureCredential](https://learn.microsoft.com/en-us/java/api/com.azure.identity.defaultazurecredential?view=azure-java-stable) class -
 * you will need to set up authentication using any of the methods listed within the Azure documentation to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary Azure infrastructure to use Azure Blob Storage as an XTDB object store, see the section on setting up
 * the [Azure Resource Manager Stack](https://github.com/xtdb/xtdb/tree/2.x/modules/azure#azure-resource-manager-stack) within our Azure docs.
 *
 * @property storageAccount The [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview)
 * which has the **container** to be used as an object store
 * @property container The name of the [container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction#containers)
 * to be used as an object store
 * @property servicebusNamespace The name of the [Service Bus namespace](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview#namespaces)
 * which contains the **servicebusTopicName** collecting notifications from the **container**
 * @property servicebusTopicName The name of the [Service Bus topic](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-queues-topics-subscriptions#topics-and-subscriptions)
 * which is collecting notifications from the **container**
 * @property prefix A file path to prefix all of your files with - for example, if "foo" is provided all xtdb files
 * will be located under a "foo" directory.
 *
 * */
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
