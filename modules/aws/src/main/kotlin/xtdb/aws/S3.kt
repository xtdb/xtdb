@file:UseSerializers(PathWithEnvVarSerde::class, StringWithEnvVarSerde::class)

package xtdb.aws

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStoreFactory
import xtdb.aws.s3.DefaultS3Configurator
import xtdb.aws.s3.S3Configurator
import xtdb.util.requiringResolve
import java.nio.file.Path

/**
 * Used to set configuration options for an S3 Object Store, which can be used as implementation of objectStore within a [xtdb.api.storage.Storage.RemoteStorageFactory].
 *
 * Requires at least [bucket][S3.Factory.bucket] and an [snsTopicArn][S3.Factory.snsTopicArn] to be provided - these will need to be accessible to whichever authentication credentials you use.
 * Authentication is handled via the Default AWS Credential Provider Chain.
 * See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) on the various methods which you can handle authentication to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary infrastructure on AWS to be able to use S3 as an XTDB object store, see the section on infrastructure & setting up the AWS Cloudformation Stack within our [S3 Module Reference](https://docs.xtdb.com/config/storage/s3.html).
 *
 * Example usage, as part of a node config:
 * ```kotlin
 * Xtdb.openNode {
 *    remoteStorage(
 *       objectStore = s3(bucket = "xtdb-bucket", snsTopicArn = "example-arn") {
 *           prefix = Path.of("my/custom/prefix")
 *       },
 *       localDiskCache = Paths.get("test-path")
 *    ),
 *    ...
 * }
 * ```
 */
object S3 {
    @JvmStatic
    fun s3(bucket: String, snsTopicArn: String) = Factory(bucket, snsTopicArn)

    /**
     * Used to set configuration options for an S3 Object Store, which can be used as implementation of objectStore within a [xtdb.api.storage.Storage.RemoteStorageFactory].
     *
     * Requires at least [bucket] and an [snsTopicArn] to be provided - these will need to be accessible to whichever authentication credentials you use.
     * Authentication is handled via the Default AWS Credential Provider Chain.
     * See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) on the various methods which you can handle authentication to be able to make use of the operations inside the modules.
     *
     * For more info on setting up the necessary infrastructure on AWS to be able to use S3 as an XTDB object store, see the section on infrastructure & setting up the AWS Cloudformation Stack within our [S3 Module Reference](https://docs.xtdb.com/config/storage/s3.html).
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    remoteStorage(
     *       objectStore = s3(bucket = "xtdb-bucket", snsTopicArn = "example-arn") {
     *           prefix = Path.of("my/custom/prefix")
     *       },
     *       localDiskCache = Paths.get("test-path")
     *    ),
     *    ...
     * }
     * ```
     * @param bucket The name of the [S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html) to be used as an object store
     * @param snsTopicArn The [ARN](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html) of the [SNS topic](https://aws.amazon.com/sns/) which is collecting notifications from the S3 bucket you are using.
     */
    @JvmSynthetic
    fun s3(bucket: String, snsTopicArn: String, configure: Factory.() -> Unit = {}) =
        s3(bucket, snsTopicArn).also(configure)

    @Serializable
    @SerialName("!S3")
    data class Factory(
        @Serializable(StringWithEnvVarSerde::class) val bucket: String,
        @Serializable(StringWithEnvVarSerde::class) val snsTopicArn: String,
        @Serializable(PathWithEnvVarSerde::class) var prefix: Path? = null,
        @Transient var s3Configurator: S3Configurator = DefaultS3Configurator,
    ) : ObjectStoreFactory {

        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        /**
         * @param s3Configurator An optional [xtdb.aws.s3.S3Configurator] instance with extra S3 configuration options to be used by the object store.
         */
        fun s3Configurator(s3Configurator: S3Configurator) = apply { this.s3Configurator = s3Configurator }

        override fun openObjectStore() = requiringResolve("xtdb.aws.s3/open-object-store")(this) as ObjectStore
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
