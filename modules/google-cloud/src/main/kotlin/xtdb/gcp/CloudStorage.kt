package xtdb.gcp

import clojure.lang.ExceptionInfo
import clojure.lang.PersistentHashMap
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStore.Companion.throwMissingKey
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage.storageRoot
import xtdb.gcp.CloudStorage.Factory
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

/**
 * Used to set configuration options for a Google Cloud Storage Object Store, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
 *
 * Requires at least [projectId][Factory.projectId] and a [bucket][Factory.bucket] to be provided - these will need to be accessible to whichever
 * authentication credentials you use. Authentication is handled via Google’s "Application Default Credentials" - see the
 * [relevant documentation](https://github.com/googleapis/google-auth-library-java/blob/main/README.md#application-default-credentials) to get set up.
 * You will need to set up authentication using any of the methods listed within the documentation to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary infrastructure on Google Cloud to be able to use Google Cloud Storage as an XTDB object store, see the section on setting up
 * the [Google Cloud Deployment Manager Configuration](https://github.com/xtdb/xtdb/tree/main/modules/google-cloud#google-cloud-deployment-manager-configuration) within our Google Cloud docs.
 *
 * Example usage, as part of a node config:
 * ```kotlin
 * Xtdb.openNode {
 *    remoteStorage(
 *       objectStore = googleCloudStorage(
 *          projectId = "xtdb-project",
 *          bucket ="xtdb-bucket"
 *       ) {
 *          prefix = Path.of("my/custom/prefix")
 *       },
 *       localDiskCache = Paths.get("test-path")
 *    ),
 *    ...
 * }
 * ```
 */
class CloudStorage(
    projectId: String,
    private val bucket: String,
    private val prefix: Path,
) : ObjectStore {

    private val client = StorageOptions.newBuilder().run { setProjectId(projectId); build() }.service

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    override fun getObject(k: Path) = scope.future {
        runInterruptible {
            val prefixedKey = prefix.resolve(k).toString()

            try {
                val blob = client.get(bucket, prefixedKey) ?: throwMissingKey(k)
                blob.getContent().let(ByteBuffer::wrap)
            } catch (e: StorageException) {
                if (e.code == 404) throwMissingKey(k)
                e.cause?.let { if (it is InterruptedException) throw it }

                throw ExceptionInfo(
                    "Error when reading object $k from bucket $bucket",
                    PersistentHashMap.create(mapOf("bucket-name" to bucket, "blob-name" to prefixedKey))
                )
            }
        }
    }

    override fun putObject(k: Path, buf: ByteBuffer) = scope.future {
        runInterruptible {
            val resolvedPath = prefix.resolve(k).toString()
            try {
                client.writer(BlobInfo.newBuilder(bucket, resolvedPath).build(), Storage.BlobWriteOption.doesNotExist())
                    .use { writer ->
                        writer.write(buf)
                    }
            } catch (e: StorageException) {
                if (e.code == 412) return@runInterruptible
                e.cause?.let { if (it is InterruptedException) throw it }

                throw ExceptionInfo(
                    "Error when writing object $k to bucket $bucket",
                    PersistentHashMap.create(mapOf("bucket-name" to bucket, "blob-name" to resolvedPath))
                )
            }
        }
    }

    override fun deleteObject(k: Path) = scope.future<Unit> {
        runInterruptible {
            client.delete(bucket, prefix.resolve(k).toString())
        }
    }

    private fun listAllObjects0(listPrefix: Path) =
        client.list(bucket, BlobListOption.prefix("$listPrefix/"))
            .iterateAll()
            .map { blob -> StoredObject(prefix.relativize(blob.name.asPath), blob.size) }

    override fun listAllObjects() = listAllObjects0(prefix)
    override fun listAllObjects(dir: Path) = listAllObjects0(prefix.resolve(dir))

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        client.close()
    }

    companion object {
        @JvmStatic
        fun googleCloudStorage(projectId: String, bucket: String) = Factory(projectId, bucket)

        @Suppress("unused")
        @JvmSynthetic
        fun googleCloudStorage(projectId: String, bucket: String, configure: Factory.() -> Unit = {}) =
            googleCloudStorage(projectId, bucket).also(configure)
    }

    /**
     * @property projectId The name of the Google Cloud Platform project that the [bucket] is contained within
     * @property bucket The name of the [Cloud Storage bucket](https://cloud.google.com/storage/docs/buckets) to use as an object store
     * @property prefix A file path to prefix all of your files with - for example, if "foo" is provided all XTDB files will be located under a "foo" directory.
     */
    @Serializable
    @SerialName("!GoogleCloud")
    data class Factory(
        val projectId: String,
        val bucket: String,
        var prefix: Path? = null,
    ) : ObjectStore.Factory {

        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        override fun openObjectStore() =
            CloudStorage(projectId, bucket, prefix?.resolve(storageRoot) ?: storageRoot) }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerObjectStore(Factory::class)
        }
    }
}