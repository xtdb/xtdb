package xtdb.azure

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import xtdb.api.RemoteAlias
import xtdb.api.Remote
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.ObjectStoreTest
import xtdb.azure.BlobStorage.Companion.azureBlobStorage
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.random.Random
import kotlin.random.nextLong
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class BlobStorageTest : ObjectStoreTest() {
    companion object {
        // Azurite's default key — an open secret published in the Azurite docs.
        private const val AZURITE_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

        private val container = GenericContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0")
            .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck")
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

        private fun azuriteEndpoint() =
            "http://${container.host}:${container.getMappedPort(10000)}/devstoreaccount1"

        private fun azuriteConnectionString() =
            "DefaultEndpointsProtocol=http;" +
                "AccountName=devstoreaccount1;" +
                "AccountKey=$AZURITE_KEY;" +
                "BlobEndpoint=${azuriteEndpoint()};"
    }

    override fun openObjectStore(prefix: Path) =
        azureBlobStorage("devstoreaccount1", "test-container") {
            storageAccountEndpoint(azuriteEndpoint())
            storageAccountKey(AZURITE_KEY)
            prefix(prefix)
        }.openObjectStore(Path("blob-store-test"))

    @Test
    fun `multipart put test`() = runTest(timeout = 10.seconds) {
        val objectStore = this@BlobStorageTest.objectStore as BlobStorage

        val multipart = objectStore.startMultipart("test-multipart".asPath)
        val part1 = randomByteBuffer(500)
        val part2 = randomByteBuffer(500)

        val parts = awaitAll(
            async { multipart.uploadPart(0, part1) },
            async { multipart.uploadPart(1, part2) }
        )

        multipart.complete(parts)
        assertTrue { objectStore.listUncommittedBlobs().toList().isEmpty() }

        assertEquals(
            setOf("test-multipart"),
            objectStore.listAllObjects().map { it.key.toString() }.toSet()
        )

        val downloaded = objectStore.getObject("test-multipart".asPath)
        assertEquals(part1.capacity() + part2.capacity(), downloaded.capacity())
    }

    private val dispatcher = Dispatchers.IO.limitedParallelism(4)

    @Test
    fun `test 20 parts`() = runTest(timeout = 10.seconds) {
        val objectStore = this@BlobStorageTest.objectStore as BlobStorage

        val upload = objectStore.startMultipart("test-20-parts".asPath)

        val parts = (0 until 20).map { randomByteBuffer(1024) }
        val totalSize = parts.sumOf { it.capacity().toLong() }

        val allParts = ByteBuffer.allocate(totalSize.toInt()).also { buffer ->
            parts.forEach { buffer.put(it.duplicate()) }
            buffer.flip()
        }

        val uploadedParts = parts.mapIndexed { idx, it ->
            async(dispatcher) {
                Thread.sleep(Random.nextLong(100L..200L))
                upload.uploadPart(idx, it)
            }
        }

        upload.complete(uploadedParts.awaitAll())

        assertTrue(objectStore.listUncommittedBlobs().toList().isEmpty(), "no uncommitted blobs")

        assertEquals(
            listOf(StoredObject("test-20-parts".asPath, totalSize)),
            objectStore.listAllObjects().toList()
        )

        val downloaded = objectStore.getObject("test-20-parts".asPath)
        assertEquals(totalSize, downloaded.capacity().toLong())
        assertEquals(allParts, downloaded)
    }

    private suspend fun roundTrip(objectStore: BlobStorage, key: String, payload: ByteBuffer) {
        objectStore.putObject(key.asPath, payload.duplicate())
        val downloaded = objectStore.getObject(key.asPath)
        assertEquals(payload, downloaded)
        assertTrue(
            objectStore.listAllObjects().map { it.key.toString() }.toSet().contains(key),
            "object $key should appear in listing"
        )
    }

    @Test
    fun `remote alias — connectionString`() = runTest {
        val remotes = mapOf<RemoteAlias, Remote>(
            "az" to AzureRemote(connectionString = azuriteConnectionString(), storageAccountKey = null),
        )

        val factory = azureBlobStorage("devstoreaccount1", "test-container") {
            remote("az")
            prefix("alias-conn-string-test".asPath)
        }

        factory.openObjectStore(Path("auth-test"), remotes).use { objectStore ->
            roundTrip(objectStore as BlobStorage, "alias-conn-roundtrip", randomByteBuffer(64))
        }
    }

    @Test
    fun `remote alias — storageAccountKey`() = runTest {
        val remotes = mapOf<RemoteAlias, Remote>(
            "az" to AzureRemote(connectionString = null, storageAccountKey = AZURITE_KEY),
        )

        val factory = azureBlobStorage("devstoreaccount1", "test-container") {
            remote("az")
            storageAccountEndpoint(azuriteEndpoint())
            prefix("alias-key-test".asPath)
        }

        factory.openObjectStore(Path("auth-test"), remotes).use { objectStore ->
            roundTrip(objectStore as BlobStorage, "alias-key-roundtrip", randomByteBuffer(64))
        }
    }
}
