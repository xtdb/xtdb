@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.storage

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.api.storage.GoogleCloudStorage.Factory
import xtdb.util.requiringResolve
import java.nio.file.Path

/**
 * Used to set configuration options for an Google Cloud Storage Object Store, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
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
object GoogleCloudStorage {
    /**
     * Used to set configuration options for an Google Cloud Storage Object Store, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
     *
     * Requires at least [projectId] and a [bucket]  - these will need to be accessible to whichever
     * authentication credentials you use. Authentication is handled via Google’s "Application Default Credentials" - see the
     * [relevant documentation](https://github.com/googleapis/google-auth-library-java/blob/main/README.md#application-default-credentials) to get set up.
     * You will need to set up authentication using any of the methods listed within the documentation to be able to make use of the operations inside the modules.
     *
     * For more info on setting up the necessary infrastructure on Google Cloud to be able to use Google Cloud Storage as an XTDB object store, see the section on setting up
     * the [Google Cloud Deployment Manager Configuration](https://github.com/xtdb/xtdb/tree/main/modules/google-cloud#google-cloud-deployment-manager-configuration) within our Google Cloud docs.
     *
     * @param projectId The name of the Google Cloud Platform project that the [bucket] is contained within
     * @param bucket The name of the [Cloud Storage bucket](https://cloud.google.com/storage/docs/buckets) to use as an object store
     */
    @JvmStatic
    fun googleCloudStorage(projectId: String, bucket: String) =
        Factory(projectId, bucket)

    /**
     * Used to set configuration options for an Google Cloud Storage Object Store, which can be used as implementation of an [object store][xtdb.api.storage.Storage.RemoteStorageFactory.objectStore].
     *
     * Requires at least [projectId] and a [bucket]  - these will need to be accessible to whichever
     * authentication credentials you use. Authentication is handled via Google’s "Application Default Credentials" - see the
     * [relevant documentation](https://github.com/googleapis/google-auth-library-java/blob/main/README.md#application-default-credentials) to get set up.
     * You will need to set up authentication using any of the methods listed within the documentation to be able to make use of the operations inside the modules.
     *
     * For more info on setting up the necessary infrastructure on Google Cloud to be able to use Google Cloud Storage as an XTDB object store, see the section on setting up
     * the [Google Cloud Deployment Manager Configuration](https://github.com/xtdb/xtdb/tree/main/modules/google-cloud#google-cloud-deployment-manager-configuration) within our Google Cloud docs.
     *
     * @param projectId The name of the Google Cloud Platform project that the [bucket] is contained within
     * @param bucket The name of the [Cloud Storage bucket](https://cloud.google.com/storage/docs/buckets) to use as an object store
     */
    @JvmSynthetic
    fun googleCloudStorage(
        projectId: String,
        bucket: String,
        configure: Factory.() -> Unit = {},
    ) = googleCloudStorage(projectId, bucket).also(configure)

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
    ) : ObjectStoreFactory {

        /**
         * @param prefix A file path to prefix all of your files with - for example, if "foo" is provided all XTDB files will be located under a "foo" directory.
         */
        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        override fun openObjectStore() =
            requiringResolve("xtdb.google-cloud/open-object-store").invoke(this) as ObjectStore
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
