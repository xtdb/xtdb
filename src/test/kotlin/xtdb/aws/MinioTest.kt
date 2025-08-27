package xtdb.aws

import clojure.java.api.Clojure
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import org.jetbrains.exposed.sql.Database.Companion.connect
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.regions.Region.AWS_ISO_GLOBAL
import xtdb.api.Xtdb
import xtdb.api.storage.Storage
import xtdb.cache.DiskCache
import xtdb.database.Database
import xtdb.symbol
import xtdb.util.asPath
import java.nio.file.Path
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@Tag("integration")
class MinioTest : S3Test() {

    companion object {
        private var wasRunning = false
        private val container = MinIOContainer("minio/minio")

        @JvmStatic
        @BeforeAll
        fun setUpMinio() {
            if (container.isRunning) wasRunning = true else container.start()

            MinioClient.builder()
                .endpoint(container.s3URL).credentials(container.userName, container.password)
                .build()
                .apply { makeBucket(MakeBucketArgs.builder().bucket(bucket).build()) }
        }

        @JvmStatic
        @AfterAll
        fun tearDownMinio() {
            if (!wasRunning) container.stop()
        }
    }

    override fun openObjectStore(prefix: Path) = S3.s3(bucket) {
        endpoint(container.s3URL)
        credentials(container.userName, container.password)
        region(AWS_ISO_GLOBAL)
        prefix(prefix)
        pathStyleAccessEnabled(true)
    }.openObjectStore()

    @Test
    fun writeBlock(@TempDir diskCacheDir: Path) {
        Clojure.`var`("clojure.core/require").invoke("xtdb.types".symbol)

        Xtdb.openNode {
            diskCache(DiskCache.factory(diskCacheDir))
            database(
                "foo",
                Database.Config()
                    .storage(
                        Storage.remote(
                            S3.s3(bucket) {
                                endpoint(container.s3URL)
                                credentials(container.userName, container.password)
                                region(AWS_ISO_GLOBAL)
                                prefix(UUID.randomUUID().toString().asPath)
                                pathStyleAccessEnabled(true)
                            }
                        )
                    )
            )
        }.use { node ->
            transaction(db = connect({ node.createConnectionBuilder().database("foo").build() })) {
                exec("INSERT INTO foo RECORDS {_id: 1}")
            }

            (node as Xtdb.XtdbInternal).dbCatalog.primary.sendFlushBlockMessage()

            transaction(db = connect(node)) {
                exec("SELECT * FROM foo.foo") { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt(1))
                    assertFalse(rs.next())
                }
            }
        }
    }
}