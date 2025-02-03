@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)

package xtdb.azure

import com.azure.core.util.BinaryData
import com.azure.identity.DefaultAzureCredential
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.BlobListDetails
import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.common.StorageSharedKeyCredential
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import reactor.core.Exceptions
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStore.Companion.throwMissingKey
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage.storageRoot
import xtdb.asBytes
import xtdb.azure.BlobStorage.Factory
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import xtdb.util.asPath
import java.io.UncheckedIOException
import java.lang.System.Logger
import java.lang.System.Logger.Level.DEBUG
import java.lang.System.Logger.Level.WARNING
import java.lang.System.LoggerFinder.getLoggerFinder
import java.nio.ByteBuffer
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Path
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.deleteIfExists
import kotlin.time.Duration.Companion.seconds

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
class BlobStorage(factory: Factory, private val prefix: Path) : ObjectStore, SupportsMultipart<String> {

    private val client =
        BlobServiceClientBuilder().run {
            factory.connectionString?.let { connectionString(it) }

            endpoint(factory.storageAccountEndpoint ?: "https://${factory.storageAccount}.blob.core.windows.net")

            credential(DefaultAzureCredentialBuilder().run {
                factory.userManagedIdentityClientId?.let { managedIdentityClientId(it) }
                build()
            })

            factory.storageAccountKey?.let { credential(StorageSharedKeyCredential(factory.storageAccount, it)) }

            buildClient()
        }.getBlobContainerClient(factory.container).also { it.createIfNotExists() }

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
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
            throw e
        }
    }

    override fun getObject(k: Path, outPath: Path) = scope.future {
        try {
            outPath.deleteIfExists()
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
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            LOGGER.log(WARNING, "Exception thrown when getting object $k", e)
            throw e
        }
    }

    override fun startMultipart(k: Path): CompletableFuture<IMultipartUpload<String>> = scope.future {
        val prefixedKey = prefix.resolve(k).toString()
        val blockBlobClient = client.getBlobClient(prefixedKey).blockBlobClient

        object : IMultipartUpload<String> {

            private val b64 = Base64.getEncoder()
            private val newBlockId get() = randomUUID().asBytes.let { b64.encodeToString(it) }

            override fun uploadPart(buf: ByteBuffer) = scope.future {
                try {
                    unwrappingReactorException {
                        val blockId = newBlockId
                        runInterruptible { blockBlobClient.stageBlock(blockId, BinaryData.fromByteBuffer(buf)) }
                        blockId
                    }
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Exception) {
                    LOGGER.log(WARNING, "Exception thrown when uploading part for object $k", e)
                    throw e
                }
            }

            override fun complete(parts: List<String>) = scope.future<Unit> {
                try {
                    unwrappingReactorException {
                        runInterruptible { blockBlobClient.commitBlockList(parts) }
                    }
                } catch (e: BlobStorageException) {
                    if (e.statusCode == 409)
                        LOGGER.log(DEBUG, "Object $k already exists")
                    else {
                        LOGGER.log(WARNING, "Exception thrown when completing multipart upload for object $k", e)
                        throw e
                    }
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Exception) {
                    LOGGER.log(WARNING, "Exception thrown when completing multipart upload for object $k", e)
                    throw e
                }
            }

            override fun abort() = completedFuture(Unit)
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

    private fun listObjects(opts: ListBlobsOptions) = sequence {
        client.listBlobs(opts, null)
            .iterableByPage()
            .forEach { yieldAll(it.value) }
    }

    private fun listAllObjects0(listPrefix: Path) =
        listObjects(ListBlobsOptions().setPrefix("$listPrefix/"))
            .map { StoredObject(prefix.relativize(it.name.asPath), it.properties.contentLength) }
            .asIterable()

    override fun listAllObjects() = listAllObjects0(prefix)
    override fun listAllObjects(dir: Path) = listAllObjects0(prefix.resolve(dir))

    // test usage only
    @Suppress("unused")
    fun listUncommittedBlobs(): Iterable<Path> {
        val committedBlobs = listAllObjects().map { it.key }.toSet()
        val uncommittedOpts =
            ListBlobsOptions()
                .setPrefix(prefix.toString())
                .setDetails(BlobListDetails().setRetrieveUncommittedBlobs(true))

        return listObjects(uncommittedOpts)
            .map { prefix.relativize(it.name.asPath) }
            .filter { it !in committedBlobs }
            .asIterable()
    }

    override fun deleteObject(k: Path) = scope.future<Unit> {
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
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }

    companion object {
        @JvmStatic
        fun azureBlobStorage(storageAccount: String, container: String) = Factory(storageAccount, container)

        @Suppress("unused")
        @JvmSynthetic
        fun azureBlobStorage(
            storageAccount: String, container: String,
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
        val storageAccount: String,
        val container: String,
        var prefix: Path? = null,
        var storageAccountKey: String? = null,
        var userManagedIdentityClientId: String? = null,
        var storageAccountEndpoint: String? = null,
        var connectionString: String? = null,
    ) : ObjectStore.Factory {

        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        @Suppress("unused")
        fun storageAccountKey(storageAccountKey: String) = apply { this.storageAccountKey = storageAccountKey }

        @Suppress("unused")
        fun userManagedIdentityClientId(clientId: String) = apply { userManagedIdentityClientId = clientId }

        @Suppress("unused")
        fun storageAccountEndpoint(endpoint: String) = apply { storageAccountEndpoint = endpoint }

        fun connectionString(connectionString: String) = apply { this.connectionString = connectionString }

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
