@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)

package xtdb.azure

import com.azure.core.util.BinaryData
import com.azure.identity.DefaultAzureCredential
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.models.ListBlobsOptions
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import reactor.core.Exceptions
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.Storage.storageRoot
import xtdb.api.storage.throwMissingKey
import xtdb.azure.BlobStorage.Factory
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import xtdb.util.requiringResolve
import java.io.UncheckedIOException
import java.lang.System.Logger
import java.lang.System.Logger.Level.DEBUG
import java.lang.System.Logger.Level.WARNING
import java.lang.System.LoggerFinder.getLoggerFinder
import java.lang.System.out
import java.nio.ByteBuffer
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Path
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture

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
class BlobStorage(factory: Factory, private val prefix: Path) : ObjectStore, SupportsMultipart {

    private val client =
        BlobServiceClientBuilder().run {
            endpoint(
                factory.storageAccountEndpoint
                    ?: factory.storageAccount?.let { "https://$it.blob.core.windows.net" }
                    ?: error("Either storageAccount or storageAccountEndpoint must be provided"))
            credential(
                DefaultAzureCredentialBuilder().run {
                    factory.userManagedIdentityClientId?.let { managedIdentityClientId(it) }
                    build()
                })
            buildClient()
        }.getBlobContainerClient(factory.container)

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    override fun getObject(k: Path) = scope.future {
        try {
            unwrappingReactorException {
                runInterruptible {
                    client.getBlobClient(prefix.resolve(k).toString())
                        .downloadContent()
                        .toByteBuffer()
                }
            }
        } catch (e: BlobStorageException) {
            if (e.statusCode == 404) throwMissingKey(k)

            LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
            throw e
        }
    }

    override fun getObject(k: Path, outPath: Path) = scope.future {
        try {
            unwrappingReactorException {
                runInterruptible {
                    client.getBlobClient(prefix.resolve(k).toString())
                        .downloadToFile(outPath.toString())
                }
            }

            outPath
        } catch (e: UncheckedIOException) {
            if (e.cause !is FileAlreadyExistsException) {
                LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
                throw e
            }

            return@future outPath
        } catch (e: BlobStorageException) {
            if (e.statusCode == 404) throwMissingKey(k)

            LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
            throw e
        }
    }

    override fun startMultipart(k: Path): CompletableFuture<IMultipartUpload> = scope.future {
        val prefixedKey = prefix.resolve(k).toString()
        val blockBlobClient = client.getBlobClient(prefixedKey).blockBlobClient

        object : IMultipartUpload {
            private val newBlockId get() = randomUUID().toString()

            private val stagedBlockIds: Channel<String> = Channel()

            override fun uploadPart(buf: ByteBuffer) = scope.future {
                try {
                    unwrappingReactorException {
                        val blockId = newBlockId
                        runInterruptible { blockBlobClient.stageBlock(blockId, BinaryData.fromByteBuffer(buf)) }
                        stagedBlockIds.send(blockId)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    LOGGER.log(WARNING, "Exception thrown when uploading part for object $k", e)
                    throw e
                }
            }

            override fun complete() = scope.future {
                try {
                    unwrappingReactorException {
                        stagedBlockIds.close()
                        val blockIds = stagedBlockIds.toList()
                        runInterruptible { blockBlobClient.commitBlockList(blockIds) }
                    }
                } catch (e: BlobStorageException) {
                    if (e.statusCode == 409)
                        LOGGER.log(DEBUG, "Object $k already exists")
                    else {
                        LOGGER.log(WARNING, "Exception thrown when completing multipart upload for object $k", e)
                        throw e
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    LOGGER.log(WARNING, "Exception thrown when completing multipart upload for object $k", e)
                    throw e
                }
            }

            override fun abort() = completedFuture(null)
        }
    }

    override fun putObject(k: Path, buf: ByteBuffer) = scope.future {
        runInterruptible {
            val prefixedKey = prefix.resolve(k).toString()
            try {
                unwrappingReactorException {
                    client.getBlobClient(prefixedKey)
                        .upload(BinaryData.fromByteBuffer(buf))
                }
            } catch (e: BlobStorageException) {
                if (e.statusCode == 409)
                    LOGGER.log(DEBUG, "Object $k already exists")
                else {
                    LOGGER.log(WARNING, "Exception thrown when putting object $k", e)
                    throw e
                }
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Exception) {
                LOGGER.log(WARNING, "Exception thrown when putting object $k", e)
                throw e
            }
        }
    }

    override fun listAllObjects() = sequence {
        client.listBlobs(ListBlobsOptions(), null)
            .iterableByPage().forEach {
                yieldAll(it.value.map { blob ->
                    ObjectStore.StoredObject(Path.of(blob.name), blob.properties.contentLength)
                })
            }
    }.asIterable()

    override fun deleteObject(k: Path) = scope.future {
        val prefixedKey = prefix.resolve(k).toString()

        runInterruptible {
            try {
                unwrappingReactorException {
                    client.getBlobClient(prefixedKey).deleteIfExists()
                }
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Exception) {
                LOGGER.log(WARNING, "Exception thrown when deleting object $k", e)
                throw e
            }
        }
    }

    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
    }

    companion object {
        @JvmStatic
        fun azureBlobStorage(
            storageAccount: String?,
            container: String,
        ) = Factory(storageAccount, container)

        @Suppress("unused")
        @JvmSynthetic
        fun azureBlobStorage(
            storageAccount: String?, container: String,
            configure: Factory.() -> Unit = {},
        ) = azureBlobStorage(storageAccount, container).also(configure)

        private inline fun <R> unwrappingReactorException(block: () -> R): R =
            try {
                block()
            } catch (e: Throwable) {
                throw Exceptions.unwrap(e)
            }

        private val LOGGER: Logger =
            getLoggerFinder().getLogger(BlobStorage::class.qualifiedName, BlobStorage::class.java.module)
    }

    @Serializable
    @SerialName("!Azure")
    data class Factory(
        val storageAccount: String? = null,
        val container: String,
        var prefix: Path? = null,
        var userManagedIdentityClientId: String? = null,
        var storageAccountEndpoint: String? = null,
    ) : ObjectStore.Factory {
        @Suppress("unused")
        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        @Suppress("unused")
        fun userManagedIdentityClientId(userManagedIdentityClientId: String) =
            apply { this.userManagedIdentityClientId = userManagedIdentityClientId }

        @Suppress("unused")
        fun storageAccountEndpoint(storageAccountEndpoint: String) =
            apply { this.storageAccountEndpoint = storageAccountEndpoint }

        override fun openObjectStore() = BlobStorage(this, prefix?.resolve(storageRoot) ?: storageRoot)
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