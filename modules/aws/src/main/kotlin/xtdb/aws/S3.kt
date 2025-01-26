@file:UseSerializers(PathWithEnvVarSerde::class, StringWithEnvVarSerde::class)

package xtdb.aws

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage.storageRoot
import xtdb.api.storage.throwMissingKey
import xtdb.aws.s3.S3Configurator
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import xtdb.util.asPath
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

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
class S3(
    factory: Factory,
    private val bucket: String,
    private val prefix: Path,
) : ObjectStore, SupportsMultipart {

    private val configurator = factory.s3Configurator

    private val client =
        S3AsyncClient.builder()
            .apply {
                factory.credentials?.let { (accessKey, secretKey) ->
                    AwsBasicCredentials.create(accessKey, secretKey)
                        .let { StaticCredentialsProvider.create(it) }
                        .also { credentialsProvider(it) }
                }
                factory.endpoint?.let { endpointOverride(URI(it)) }

                configurator.configureClient(this)
            }.build()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        client.close()
    }

    override fun startMultipart(k: Path): CompletableFuture<IMultipartUpload> = scope.future {
        val s3Key = prefix.resolve(k).toString()
        val initResp = client.createMultipartUpload {
            it.bucket(bucket)
            it.key(s3Key)
        }.await()

        val uploadId = initResp.uploadId()

        object : IMultipartUpload {
            val partNum = AtomicInteger(1)
            val parts = Channel<CompletedPart>(UNLIMITED)

            fun S3AsyncClient.uploadPart(body: AsyncRequestBody, configure: Consumer<UploadPartRequest.Builder>) =
                uploadPart(configure, body)

            override fun uploadPart(buf: ByteBuffer) = scope.future {
                val contentLength = buf.remaining().toLong()
                val partNum = partNum.getAndIncrement()

                val partResp = client.uploadPart(AsyncRequestBody.fromByteBuffer(buf)) {
                    it.bucket(bucket)
                    it.key(s3Key)
                    it.uploadId(uploadId)
                    it.partNumber(partNum)
                    it.contentLength(contentLength)
                }.await()

                parts.send(
                    CompletedPart.builder().apply {
                        partNumber(partNum)
                        eTag(partResp.eTag())
                    }.build()
                )
            }

            override fun complete() = scope.future {
                parts.close()
                val parts = parts.toList().sortedBy { it.partNumber() }

                client.completeMultipartUpload { req ->
                    req.bucket(bucket)
                    req.key(s3Key)
                    req.uploadId(uploadId)
                    req.multipartUpload { it.parts(parts) }
                }.await()
            }

            override fun abort() = scope.future {
                client.abortMultipartUpload {
                    it.bucket(bucket)
                    it.key(s3Key)
                    it.uploadId(uploadId)
                }.await()
            }
        }
    }

    private fun getObjectRequest(k: Path) =
        GetObjectRequest.builder().run {
            bucket(bucket)
            key(prefix.resolve(k).toString())
            configurator.configureGet(this)
            build()
        }

    private fun <R> getObject(k: Path, responseTransformer: AsyncResponseTransformer<GetObjectResponse, R>) =
        client.getObject(getObjectRequest(k), responseTransformer)
            .exceptionally {
                if (it is NoSuchKeyException || it.cause is NoSuchKeyException) throwMissingKey(k) else throw it
            }

    override fun getObject(k: Path) = scope.future {
        getObject(k, AsyncResponseTransformer.toBytes()).await().asByteBuffer()
    }

    override fun getObject(k: Path, outPath: Path) = scope.future {
        getObject(k, AsyncResponseTransformer.toFile(outPath)).await()
        outPath
    }

    private fun S3AsyncClient.putObject(body: AsyncRequestBody, configure: Consumer<PutObjectRequest.Builder>) =
        putObject(configure, body)

    override fun putObject(k: Path, buf: ByteBuffer) =
        scope.future {
            val s3Key = prefix.resolve(k).toString()
            val headResp = runCatching {
                client.headObject {
                    it.bucket(bucket)
                    it.key(s3Key)
                    configurator.configureHead(it)
                }.await()
            }.exceptionOrNull()

            if (headResp == null) return@future
            if (headResp !is NoSuchKeyException && headResp.cause !is NoSuchKeyException) throw headResp

            val contentLength = buf.remaining().toLong()

            client.putObject(AsyncRequestBody.fromByteBuffer(buf)) {
                it.bucket(bucket)
                it.key(s3Key)
                it.contentLength(contentLength)
                configurator.configurePut(it)
            }.await()
        }

    override fun listAllObjects(): Iterable<StoredObject> =
        sequence {
            var continuationToken: String? = null

            while (true) {
                val listResp = runBlocking {
                    client.listObjectsV2 {
                        it.bucket(bucket)
                        it.prefix(prefix.toString())
                        it.continuationToken(continuationToken)
                    }.await()
                }

                yieldAll(listResp.contents().map { StoredObject(prefix.relativize(it.key().asPath), it.size()) })

                if (!listResp.isTruncated) break
                continuationToken = listResp.nextContinuationToken()
            }
        }.asIterable()

    // used for multipart upload testing
    fun listUploads(): Set<Path> = runBlocking {
        client.listMultipartUploads {
            it.bucket(bucket)
            it.prefix(prefix.toString())
        }.await()
            .uploads()
            .map { prefix.relativize(it.key().asPath) }
            .toSet()
    }

    override fun deleteObject(k: Path): CompletableFuture<*> =
        client.deleteObject {
            it.bucket(bucket)
            it.key(prefix.resolve(k).toString())
        }

    companion object {
        @JvmStatic
        fun s3(bucket: String) = Factory(bucket)

        @Suppress("unused")
        @JvmSynthetic
        fun s3(bucket: String, configure: Factory.() -> Unit = {}) =
            s3(bucket).also(configure)

        @Serializable
        data class BasicCredentials(
            @Serializable(StringWithEnvVarSerde::class) val accessKey: String,
            @Serializable(StringWithEnvVarSerde::class) val secretKey: String
        )
    }

    @Serializable
    @SerialName("!S3")
    data class Factory(
        @Serializable(StringWithEnvVarSerde::class) val bucket: String,
        @Serializable(PathWithEnvVarSerde::class) var prefix: Path? = null,
        var credentials: BasicCredentials? = null,
        @Serializable(StringWithEnvVarSerde::class) var endpoint: String? = null,
        @Transient var s3Configurator: S3Configurator = S3Configurator.Default,
    ) : ObjectStore.Factory {

        fun prefix(prefix: Path) = apply { this.prefix = prefix }

        fun credentials(accessKey: String, secretKey: String) =
            apply { credentials = BasicCredentials(accessKey, secretKey) }

        fun endpoint(endpoint: String) = apply { this.endpoint = endpoint }

        fun s3Configurator(s3Configurator: S3Configurator) = apply { this.s3Configurator = s3Configurator }

        override fun openObjectStore() = S3(this, bucket, prefix?.resolve(storageRoot) ?: storageRoot)
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
