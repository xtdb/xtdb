@file:UseSerializers(PathWithEnvVarSerde::class, StringWithEnvVarSerde::class)

package xtdb.aws

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
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
 * Requires at least a [bucket][S3.Factory.bucket] - this will need to be accessible to whichever authentication credentials you use.
 * Authentication is handled via the Default AWS Credential Provider Chain.
 * See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) on the various methods which you can handle authentication to be able to make use of the operations inside the modules.
 *
 * For more info on setting up the necessary infrastructure on AWS to be able to use S3 as an XTDB object store, see the section on infrastructure & setting up the AWS Cloudformation Stack within our [S3 Module Reference](https://docs.xtdb.com/config/storage/s3.html).
 *
 * Example usage, as part of a node config:
 * ```kotlin
 * Xtdb.openNode {
 *    remoteStorage(
 *       objectStore = s3(bucket = "xtdb-bucket") {
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
    fun s3(bucket: String) = Factory(bucket)

    /**
     * Used to set configuration options for an S3 Object Store, which can be used as implementation of objectStore within a [xtdb.api.storage.Storage.RemoteStorageFactory].
     *
     * Requires at least a [bucket] - this will need to be accessible to whichever authentication credentials you use.
     * Authentication is handled via the Default AWS Credential Provider Chain.
     * See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) on the various methods which you can handle authentication to be able to make use of the operations inside the modules.
     *
     * For more info on setting up the necessary infrastructure on AWS to be able to use S3 as an XTDB object store, see the section on infrastructure & setting up the AWS Cloudformation Stack within our [S3 Module Reference](https://docs.xtdb.com/config/storage/s3.html).
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    remoteStorage(
     *       objectStore = s3(bucket = "xtdb-bucket") {
     *           prefix = Path.of("my/custom/prefix")
     *       },
     *       localDiskCache = Paths.get("test-path")
     *    ),
     *    ...
     * }
     * ```
     * @param bucket The name of the [S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html) to be used as an object store
     */
    @JvmSynthetic
    fun s3(bucket: String, configure: Factory.() -> Unit = {}) =
        s3(bucket).also(configure)

    @Serializable
    data class BasicCredentials(
        @Serializable(StringWithEnvVarSerde::class) val accessKey: String,
        @Serializable(StringWithEnvVarSerde::class) val secretKey: String
    )

    @Serializable
    @SerialName("!S3")
    data class Factory(
        @Serializable(StringWithEnvVarSerde::class) val bucket: String,
        @Serializable(PathWithEnvVarSerde::class) var prefix: Path? = null,
        var credentials: BasicCredentials? = null,
        @Serializable(StringWithEnvVarSerde::class) var endpoint: String? = null,
        @Transient var s3Configurator: S3Configurator = DefaultS3Configurator,
    ) : ObjectStoreFactory {

        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        fun credentials(accessKey: String, secretKey: String) =
            apply { this.credentials = BasicCredentials(accessKey, secretKey) }

        fun endpoint(endpoint: String) = apply { this.endpoint = endpoint }

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

fun foo(accessKey: String, secretKey: String) {

    S3AsyncClient.builder().credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
    ).build()
}