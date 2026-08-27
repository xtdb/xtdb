package xtdb.aws

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.http.SdkHttpFullResponse
import software.amazon.awssdk.http.SdkHttpMethod.HEAD
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.http.async.AsyncExecuteRequest
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import xtdb.aws.S3.Companion.s3
import xtdb.aws.s3.S3Configurator
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * The signer only skips payload signing over HTTPS - over plain HTTP it hashes the payload regardless of
 * configuration - so this stands in a fake transport rather than pointing at the HTTP MinIO the other S3
 * tests use, which would exercise a different branch from production.
 */
class S3UploadSigningTest {

    private class StubTransport : S3Configurator, SdkAsyncHttpClient {
        val requests = ConcurrentHashMap<String, SdkHttpRequest>()

        override fun configureClient(builder: S3AsyncClientBuilder) {
            builder.httpClient(this)
        }

        override fun execute(request: AsyncExecuteRequest): CompletableFuture<Void> {
            val httpRequest = request.request()
            requests.putIfAbsent(httpRequest.method().name, httpRequest)

            // a real 404 rather than a synthesised NoSuchKeyException: the SDK translates one into the other
            // for HeadObject, and that translation reads headers off the response the exception came from.
            val status = if (httpRequest.method() == HEAD) 404 else 200

            request.responseHandler().apply {
                onHeaders(SdkHttpFullResponse.builder().statusCode(status).build())
                onStream(EmptyBody)
            }

            return CompletableFuture.completedFuture(null)
        }

        override fun clientName() = "stub"
        override fun close() = Unit
    }

    private object EmptyBody : Publisher<ByteBuffer> {
        override fun subscribe(subscriber: Subscriber<in ByteBuffer>) {
            subscriber.onSubscribe(object : Subscription {
                private var done = false
                override fun request(n: Long) {
                    if (!done) { done = true; subscriber.onComplete() }
                }

                override fun cancel() = Unit
            })
        }
    }

    private fun uploadTo(endpoint: String): SdkHttpRequest {
        val transport = StubTransport()

        @Suppress("DEPRECATION")
        val factory = s3("test-bucket") {
            region("us-east-1")
            credentials("test-access", "test-secret")
            endpoint(endpoint)
            s3Configurator(transport)
        }

        factory.openObjectStore("upload-signing-test".asPath).use { store ->
            runBlocking { store.putObject("foo".asPath, ByteBuffer.allocate(4096)) }
        }

        return transport.requests["PUT"] ?: fail("no PUT was issued - saw ${transport.requests.keys}")
    }

    @Test
    fun `an upload carries neither chunked encoding nor a checksum, so the SDK copies nothing`() {
        val put = uploadTo("https://s3.example.com")

        assertNull(
            put.firstMatchingHeader("Content-Encoding").orElse(null),
            "aws-chunked framing allocates a heap buffer per chunk"
        )

        assertNull(
            put.headers().keys.firstOrNull { it.startsWith("x-amz-checksum-", ignoreCase = true) },
            "a request checksum makes the signer buffer the whole payload in memory"
        )

        assertEquals(
            "UNSIGNED-PAYLOAD", put.firstMatchingHeader("x-amz-content-sha256").orElse(null),
            "payload signing hashes the whole payload before transmitting it"
        )
    }

    @Test
    fun `an upload to a plain-HTTP endpoint keeps chunked encoding`() {
        val put = uploadTo("http://localhost:9000")

        assertEquals(
            "aws-chunked", put.firstMatchingHeader("Content-Encoding").orElse(null),
            "HTTP forces payload signing, and only chunk framing keeps that off the whole payload at once"
        )
    }
}
