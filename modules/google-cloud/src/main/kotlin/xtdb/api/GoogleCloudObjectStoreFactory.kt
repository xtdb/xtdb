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

/**
 * Used to set configuration options for an Google Cloud Storage Object Store,
 * which can be used as implementation of objectStore within a [xtdb.api.storage.RemoteStorageFactory].
 *
 * Requires at least **projectId**, a **bucket** and a **pubSubTopic** to be provided - these will need to be accessible to whichever
 * authentication credentials you use. Authentication is handled via Google’s "Application Default Credentials" - see the
 * [*relevant documentation*](https://github.com/googleapis/google-auth-library-java/blob/main/README.md#application-default-credentials) to get set up.
 * You will need to set up authentication using any of the methods listed within the documentation to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary infrastructure on Google Cloud to be able to use Google Cloud Storage as an XTDB object store, see the section on setting up
 * the [Google Cloud Deployment Manager Configuration](https://github.com/xtdb/xtdb/tree/2.x/modules/google-cloud#google-cloud-deployment-manager-configuration) within our Google Cloud docs.
 *
 * @property projectId The name of the Google Cloud Platform project that the **bucket** is contained within
 * @property bucket The name of the [Cloud Storage bucket](https://cloud.google.com/storage/docs/buckets) to use as an object store
 * @property pubsubTopic The name of the [Pub/Sub topic](https://cloud.google.com/pubsub/docs/overview#core_concepts) which is collecting notifications from the Cloud Storage **bucket**
 * @property prefix A file path to prefix all of your files with - for example, if "foo" is provided all xtdb files
 * will be located under a "foo" directory.
 *
 * */
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
