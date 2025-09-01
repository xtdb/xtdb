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
import xtdb.api.log.Log
import xtdb.api.storage.Storage
import xtdb.cache.DiskCache
import xtdb.database.Database
import xtdb.symbol
import xtdb.util.asPath
import java.nio.file.Path
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

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
    fun writeBlock(@TempDir nodeDir: Path) {
        Clojure.`var`("clojure.core/require").invoke("xtdb.types".symbol)

        val xtdbLog = Log.localLog(nodeDir.resolve("xt-log"))
        val xtdbStorage = Storage.local(nodeDir.resolve("xt-objs"))

        Xtdb.openNode {
            diskCache(DiskCache.factory(nodeDir.resolve("disk-cache")))

            log(xtdbLog)
            storage(xtdbStorage)
        }.use { node ->
            node.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("""ATTACH DATABASE foo WITH $$
                        log: !Local { path: "${nodeDir.resolve("foo-log")}" }
                        storage: !Remote 
                          objectStore: !S3 
                            bucket: "$bucket"
                            endpoint: "${container.s3URL}"
                            region: "${AWS_ISO_GLOBAL.id()}"
                            credentials:
                              accessKey: "${container.userName}"
                              secretKey: "${container.password}"
                            pathStyleAccessEnabled: true
                            prefix: "${UUID.randomUUID()}"
                        $$"""
                    )
                }
            }

            transaction(db = connect({ node.createConnectionBuilder().database("foo").build() })) {
                exec("INSERT INTO foo RECORDS {_id: 1}")
            }

            (node as Xtdb.XtdbInternal).dbCatalog.let { cat ->
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
        }.use { node ->
            val dbCatalog = (node as Xtdb.XtdbInternal).dbCatalog

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
}