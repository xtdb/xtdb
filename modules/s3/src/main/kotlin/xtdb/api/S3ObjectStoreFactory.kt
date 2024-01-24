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

/**
 * Used to set configuration options for an S3 Object Store,
 * which can be used as implementation of objectStore within a [xtdb.api.storage.RemoteStorageFactory].
 *
 * Requires at least **bucket** and an **snsTopicArn** to be provided - these will need to be accessible to whichever
 * authentication credentials you use. Authentication is handled via the Default AWS Credential Provider Chain.
 * See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default)
 * on the various methods which you can handle authentication to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary infrastructure on AWS to be able to use S3 as an XTDB object store, see the section on setting up
 * the [Cloudformation Stack](https://github.com/xtdb/xtdb/tree/2.x/modules/s3#cloudformation-stack) within our S3 docs.
 *
 * @property bucket The name of the [S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html) to used as an object store
 * @property snsTopicArn The [ARN](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html) of the
 * [SNS topic](https://aws.amazon.com/sns/) which is collecting notifications from the S3 bucket you are using.
 * @property prefix A file path to prefix all of your files with - for example, if "foo" is provided all xtdb files
 * will be located under a "foo" directory.
 * @property s3Configurator An optional [xtdb.s3.S3Configurator] instance with extra s3 configuration options to be used by the object store.
 *
 * */
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
