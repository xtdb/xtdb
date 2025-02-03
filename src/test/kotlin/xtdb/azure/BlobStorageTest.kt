package xtdb.azure

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.ObjectStoreTest
import xtdb.azure.BlobStorage.Companion.azureBlobStorage
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.random.Random
import kotlin.random.nextLong
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class BlobStorageTest : ObjectStoreTest() {
    companion object {
        private val container = GenericContainer("mcr.microsoft.com/azure-storage/azurite")
            .withExposedPorts(10000)

        @JvmStatic
        @BeforeAll
        fun setUpAzure() {
            container.start()
        }

        @JvmStatic
        @AfterAll
        fun tearDownAzure() {
            container.stop()
        }
    }

    override fun openObjectStore(prefix: Path) =
        azureBlobStorage("devstoreaccount1", "test-container") {
            val host = Companion.container.host
            val port = Companion.container.getMappedPort(10000)

            storageAccountEndpoint("http://$host:$port/devstoreaccount1")

            // Azurite's default key - an open secret
            storageAccountKey("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

            prefix(prefix)
        }.openObjectStore()

    @Test
    fun `multipart put test`() = runTest(timeout = 10.seconds) {
        val objectStore = this@BlobStorageTest.objectStore as BlobStorage

        val multipart = objectStore.startMultipart("test-multipart".asPath).await()
        val part1 = randomByteBuffer(500)
        val part2 = randomByteBuffer(500)

        val parts = awaitAll(
            async { multipart.uploadPart(part1).await() },
            async { multipart.uploadPart(part2).await() }
        )

        multipart.complete(parts).await()
        assertTrue { objectStore.listUncommittedBlobs().toList().isEmpty() }

        assertEquals(
            setOf("test-multipart"),
            objectStore.listAllObjects().map { it.key.toString() }.toSet()
        )

        val downloaded = objectStore.getObject("test-multipart".asPath).await()
        assertEquals(part1.capacity() + part2.capacity(), downloaded.capacity())
    }

    private val dispatcher = Dispatchers.IO.limitedParallelism(4)

    @Test
    fun `test 20 parts`() = runTest(timeout = 10.seconds) {
        val objectStore = this@BlobStorageTest.objectStore as BlobStorage

        val upload = objectStore.startMultipart("test-20-parts".asPath).await()

        val parts = (0 until 20).map { randomByteBuffer(1024) }
        val totalSize = parts.sumOf { it.capacity().toLong() }

        val allParts = ByteBuffer.allocate(totalSize.toInt()).also { buffer ->
            parts.forEach { buffer.put(it.duplicate()) }
            buffer.flip()
        }

        val uploadedParts = parts.map {
            async(dispatcher) {
                Thread.sleep(Random.nextLong(100L..200L))
                upload.uploadPart(it).await()
            }
        }

        upload.complete(uploadedParts.awaitAll()).await()

        assertTrue(objectStore.listUncommittedBlobs().toList().isEmpty(), "no uncommitted blobs")

        assertEquals(
            listOf(StoredObject("test-20-parts".asPath, totalSize)),
            objectStore.listAllObjects().toList()
        )

        val downloaded = objectStore.getObject("test-20-parts".asPath).await()
        assertEquals(totalSize, downloaded.capacity().toLong())
        assertEquals(allParts, downloaded)
    }
}
