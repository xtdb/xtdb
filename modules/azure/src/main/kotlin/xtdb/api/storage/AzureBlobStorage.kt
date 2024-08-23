@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.storage

import com.azure.identity.DefaultAzureCredential
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.api.storage.AzureBlobStorage.Factory
import xtdb.util.requiringResolve
import java.nio.file.Path

/**
 * Used to set configuration options for Azure Blob Storage, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
 *
 * Requires at least [storageAccount][Factory.storageAccount] and [container][Factory.container] to be provided - these will need to be accessible to whichever authentication credentials you use.
 * Authentication for the components in the module is done via the [DefaultAzureCredential] class - you will need to set up authentication using any of the methods listed within the Azure documentation to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary Azure infrastructure to use Azure Blob Storage as an XTDB object store, see the section on setting up the [Azure Resource Manager Stack](https://github.com/xtdb/xtdb/tree/main/modules/azure#azure-resource-manager-stack) within our Azure docs.
 *
 * Example usage, as part of a node config:
 * ```kotlin
 * Xtdb.openNode {
 *    remoteStorage(
 *       objectStore = azureBlobStorage(
 *          storageAccount = "xtdb-storage-account",
 *          container = "xtdb-container",
 *       ) {
 *          prefix = Path.of("my/custom/prefix")
 *          userManagedIdentityClientId = "user-managed-identity-client-id"
 *          storageAccountEndpoint = "https://xtdb-storage-account.privatelink.blob.core.windows.net"
 *       },
 *       localDiskCache = Paths.get("test-path")
 *    ),
 *    ...
 * }
 * ```
 */
object AzureBlobStorage {
    /**
     * Used to set configuration options for Azure Blob Storage, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
     *
     * The [storageAccount] and [container] will need to be accessible to whichever authentication credentials you use.
     * Authentication for the components in the module is done via the [DefaultAzureCredential] class - you will need to set up authentication using any of the methods listed within the Azure documentation to be able to make use of the operations inside the modules.
     *
     * For more info on setting up the necessary Azure infrastructure to use Azure Blob Storage as an XTDB object store, see the section on setting up the [Azure Resource Manager Stack](https://github.com/xtdb/xtdb/tree/main/modules/azure#azure-resource-manager-stack) within our Azure docs.
     *
     * @param storageAccount The [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview) which has the [container] to be used as an object store
     * @param container The name of the [container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction#containers) to be used as an object store
     */
    @JvmStatic
    fun azureBlobStorage(
        storageAccount: String?,
        container: String,
    ) = Factory(storageAccount, container)

    /**
     * Used to set configuration options for Azure Blob Storage, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
     *
     * The [storageAccount] and [container] will need to be accessible to whichever authentication credentials you use.
     * Authentication for the components in the module is done via the [DefaultAzureCredential] class - you will need to set up authentication using any of the methods listed within the Azure documentation to be able to make use of the operations inside the modules.
     *
     * For more info on setting up the necessary Azure infrastructure to use Azure Blob Storage as an XTDB object store, see the section on setting up the [Azure Resource Manager Stack](https://github.com/xtdb/xtdb/tree/main/modules/azure#azure-resource-manager-stack) within our Azure docs.
     *
     * @param storageAccount The [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview) which has the [container] to be used as an object store
     * @param container The name of the [container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction#containers) to be used as an object store
     */
    @Suppress("unused")
    @JvmSynthetic
    fun azureBlobStorage(
        storageAccount: String?, container: String,
        configure: Factory.() -> Unit = {},
    ) = azureBlobStorage(storageAccount, container).also(configure)

    /**
     * @property storageAccount The [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview) which has the [container] to be used as an object store
     * @property container The name of the [container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction#containers) to be used as an object store
     * @property prefix A file path to prefix all of your files with - for example, if "foo" is provided all xtdb files will be located under a "foo" directory.
     * @property userManagedIdentityClientId The client ID of the user managed identity to use for authentication, if applicable
     * @property storageAccountEndpoint The full endpoint of the [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview) which has the [container] to be used as an object store
     */
    @Serializable
    @SerialName("!Azure")
    data class Factory(
        val storageAccount: String? = null,
        val container: String,
        var prefix: Path? = null,
        var userManagedIdentityClientId: String? = null,
        var storageAccountEndpoint: String? = null,
    ) : ObjectStoreFactory {
        /**
         * @param prefix A file path to prefix all of your files with - for example, if "foo" is provided all xtdb files will be located under a "foo" directory.
         */
        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        /**
         * @param userManagedIdentityClientId The client ID of the user managed identity to use for authentication, if applicable
         */
        fun userManagedIdentityClientId(userManagedIdentityClientId: String) = apply { this.userManagedIdentityClientId = userManagedIdentityClientId }

        /**
         * @param storageAccountEndpoint The full endpoint of the [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview) which has the [container] to be used as an object store
         */
        fun storageAccountEndpoint(storageAccountEndpoint: String) = apply { this.storageAccountEndpoint = storageAccountEndpoint }

        override fun openObjectStore() = requiringResolve("xtdb.azure/open-object-store")(this) as ObjectStore
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerObjectStore(Factory::class)
        }
    }
}
