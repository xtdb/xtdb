package xtdb.aws

import clojure.java.api.Clojure
import kotlinx.coroutines.test.runTest
import org.gaul.s3proxy.BlobStores
import org.gaul.s3proxy.S3Proxy
import org.gaul.s3proxy.auth.AuthenticationType
import org.jetbrains.exposed.v1.jdbc.Database.Companion.connect
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region.AWS_ISO_GLOBAL
import software.amazon.awssdk.services.s3.S3Client
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage
import xtdb.cache.DiskCache
import xtdb.symbol
import xtdb.util.asPath
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.*
import kotlin.io.path.Path
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Tag("integration")
class S3ProxyTest : S3Test() {

    companion object {
        private const val ACCESS_KEY = "xtdb"
        private const val SECRET_KEY = "xtdb-secret-key"
        private const val PART_SIZE = 5 * 1024 * 1024

        private val proxy: S3Proxy = S3Proxy.builder()
            .blobStore(BlobStores.create("transient", Properties()))
            .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, ACCESS_KEY, SECRET_KEY)
            .endpoint(URI.create("http://127.0.0.1:0"))
            .stopTimeout(0)
            .build()

        private val proxyEndpoint: String get() = "http://127.0.0.1:${proxy.port}"

        @JvmStatic
        @BeforeAll
        fun setUpS3Proxy() {
            proxy.start()

            S3Client.builder()
                .endpointOverride(URI(proxyEndpoint))
                .region(AWS_ISO_GLOBAL)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .build()
                .use { it.createBucket { req -> req.bucket(bucket) } }
        }

        @JvmStatic
        @AfterAll
        fun tearDownS3Proxy() = proxy.stop()
    }

    override fun openObjectStore(prefix: Path) = S3.s3(bucket) {
        endpoint(proxyEndpoint)
        credentials(ACCESS_KEY, SECRET_KEY)
        region(AWS_ISO_GLOBAL)
        prefix(prefix)
        pathStyleAccessEnabled(true)
    }.openObjectStore(Path("test-root"))

    @Test
    fun `multipart put test`() = runTest(timeout = 10.seconds) {
        val objectStore = this@S3ProxyTest.objectStore as S3

        val part1 = randomByteBuffer(PART_SIZE)
        val part2 = randomByteBuffer(PART_SIZE)

        val upload = objectStore.startMultipart("test-multipart".asPath)
        upload.complete(listOf(upload.uploadPart(0, part1.duplicate()), upload.uploadPart(1, part2.duplicate())))

        assertEquals(emptySet(), objectStore.listUploads())

        assertEquals(
            listOf(StoredObject("test-multipart".asPath, 2L * PART_SIZE)),
            objectStore.listAllObjects().toList()
        )

        val expected = ByteBuffer.allocate(2 * PART_SIZE).put(part1.duplicate()).put(part2.duplicate()).flip()
        assertEquals(expected, objectStore.getObject("test-multipart".asPath))
    }

    @Test
    fun `an aborted multipart upload leaves neither the upload nor an object behind`() = runTest(timeout = 10.seconds) {
        val objectStore = this@S3ProxyTest.objectStore as S3

        val upload = objectStore.startMultipart("test-aborted".asPath)
        upload.uploadPart(0, randomByteBuffer(PART_SIZE))
        assertEquals(setOf("test-aborted".asPath), objectStore.listUploads())

        upload.abort()

        assertEquals(emptySet(), objectStore.listUploads())
        assertEquals(emptyList(), objectStore.listAllObjects().toList())
    }

    @Test
    fun writeBlock(@TempDir nodeDir: Path) = runTest {
        Clojure.`var`("clojure.core/require").invoke("xtdb.types".symbol)

        val xtdbLog = Log.localLog(nodeDir.resolve("xt-log"))
        val xtdbStorage = Storage.local(nodeDir.resolve("xt-objs"))

        Xtdb.openNode {
            diskCache(DiskCache.factory(nodeDir.resolve("disk-cache")))
            log(xtdbLog)
            storage(xtdbStorage)
            compactor { threads(0) }
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("""ATTACH DATABASE foo WITH $$
                        log: !Local { path: "${nodeDir.resolve("foo-log")}" }
                        storage: !Remote 
                          objectStore: !S3 
                            bucket: "$bucket"
                            endpoint: "$proxyEndpoint"
                            region: "${AWS_ISO_GLOBAL.id()}"
                            credentials:
                              accessKey: "$ACCESS_KEY"
                              secretKey: "$SECRET_KEY"
                            pathStyleAccessEnabled: true
                            prefix: "${UUID.randomUUID()}"
                        $$"""
                    )
                }
            }

            transaction(db = connect({ node.createConnectionBuilder().database("foo").build() })) {
                exec("INSERT INTO foo RECORDS {_id: 1}")
            }

            (node as XtdbInternal).dbCatalog.let { cat ->
                cat.primary.sendFlushBlockMessage()
                cat["foo"]!!.sendFlushBlockMessage()
                cat.syncAll(2.seconds.toJavaDuration())
            }

            transaction(db = connect(node)) {
                exec("SELECT * FROM foo.foo") { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt(1))
                    assertFalse(rs.next())
                }
            }
        }

        Xtdb.openNode {
            diskCache(DiskCache.factory(nodeDir.resolve("disk-cache2")))
            log(xtdbLog)
            storage(xtdbStorage)
            compactor { threads(0) }
        }.use { node ->
            val dbCatalog = (node as XtdbInternal).dbCatalog

            assertEquals(setOf("xtdb", "foo"), dbCatalog.databaseNames.toSet())

            transaction(db = connect(node)) {
                exec("SELECT * FROM foo.foo") { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt(1))
                    assertFalse(rs.next())
                }
            }
        }
    }

    @Test
    fun `attach with credential-less S3 storage resolves via the default chain`(@TempDir nodeDir: Path) = runTest {
        // the attach !S3 block omits credentials, so the object store must fall through to the
        // default AWS provider chain (IRSA in production) - seed that chain via system properties
        val prevAccessKey = System.setProperty("aws.accessKeyId", ACCESS_KEY)
        val prevSecretKey = System.setProperty("aws.secretAccessKey", SECRET_KEY)
        try {
            Xtdb.openNode {
                diskCache(DiskCache.factory(nodeDir.resolve("disk-cache")))
                log(Log.localLog(nodeDir.resolve("xt-log")))
                storage(Storage.local(nodeDir.resolve("xt-objs")))
                compactor { threads(0) }
            }.use { node ->
                node.connection.use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.execute("""ATTACH DATABASE foo WITH $$
                            log: !Local { path: "${nodeDir.resolve("foo-log")}" }
                            storage: !Remote
                              objectStore: !S3
                                bucket: "$bucket"
                                endpoint: "$proxyEndpoint"
                                region: "${AWS_ISO_GLOBAL.id()}"
                                pathStyleAccessEnabled: true
                                prefix: "${UUID.randomUUID()}"
                            $$"""
                        )
                    }
                }

                val cat = (node as XtdbInternal).dbCatalog
                assertEquals(setOf("xtdb", "foo"), cat.databaseNames.toSet())
                assertNull(cat["foo"]?.ingestionError)
            }
        } finally {
            prevAccessKey?.let { System.setProperty("aws.accessKeyId", it) } ?: System.clearProperty("aws.accessKeyId")
            prevSecretKey?.let { System.setProperty("aws.secretAccessKey", it) } ?: System.clearProperty("aws.secretAccessKey")
        }
    }
}