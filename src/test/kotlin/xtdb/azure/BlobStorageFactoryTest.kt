package xtdb.azure

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.nodeConfig
import xtdb.azure.BlobStorage.Companion.azureBlobStorage
import xtdb.api.error.Incorrect
import xtdb.util.asPath
import kotlin.io.path.Path

class BlobStorageFactoryTest {

    private val errorCodeKey = Keyword.intern("xtdb.error", "code")
    private fun Incorrect.errorCode() = data.valAt(errorCodeKey) as Keyword?

    private class StubRemote : Remote {
        override fun close() = Unit
    }

    @Test
    fun `proto round trip with inline credentials`() {
        val original = azureBlobStorage("test-storage", "test-container") {
            prefix("test/prefix".asPath)
            storageAccountKey("test-key")
            userManagedIdentityClientId("test-client-id")
            storageAccountEndpoint("https://test.blob.core.windows.net")
        }

        val restored = BlobStorage.Registration().fromProto(original.configProto)
        assertEquals(original, restored)
    }

    @Test
    fun `proto round trip — alias path strips credentials`() {
        // The leak-fix invariant: when the alias path is in use, the proto
        // must not carry connectionString or storageAccountKey — those are
        // node-local and would otherwise land on the source log.
        val original = azureBlobStorage("test-storage", "test-container") {
            prefix("test/prefix".asPath)
            remote("az")
        }

        val proto = original.configProto.unpack(xtdb.azure.proto.AzureBlobStorageConfig::class.java)
        assertEquals("az", proto.remote)
        assertEquals("", proto.storageAccountKey, "storageAccountKey must not appear on alias path")

        val restored = BlobStorage.Registration().fromProto(original.configProto)
        assertEquals("az", restored.remote)
        assertNull(restored.storageAccountKey)
    }

    @Test
    fun `openObjectStore errors when both remote and inline creds set`() {
        val factory = azureBlobStorage("test-storage", "test-container") {
            remote("az")
            storageAccountKey("test")
        }

        val e = assertThrows<Incorrect> {
            factory.openObjectStore(Path("storage-root"), emptyMap())
        }
        assertEquals(Keyword.intern("xtdb.azure", "conflicting-config"), e.errorCode())
    }

    @Test
    fun `openObjectStore errors on missing remote alias`() {
        val factory = azureBlobStorage("test-storage", "test-container") {
            remote("az")
        }

        val e = assertThrows<Incorrect> {
            factory.openObjectStore(Path("storage-root"), emptyMap())
        }
        assertEquals(Keyword.intern("xtdb.azure", "missing-remote"), e.errorCode())
    }

    @Test
    fun `openObjectStore errors on wrong remote type`() {
        val factory = azureBlobStorage("test-storage", "test-container") {
            remote("az")
        }

        val remotes: Map<RemoteAlias, Remote> = mapOf("az" to StubRemote())
        val e = assertThrows<Incorrect> {
            factory.openObjectStore(Path("storage-root"), remotes)
        }
        assertEquals(Keyword.intern("xtdb.azure", "wrong-remote-type"), e.errorCode())
    }

    @Test
    fun `node config decodes !AzureBlob remote under remotes`() {
        val yaml = """
            remotes:
              az: !AzureBlob
                connectionString: DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=secret;BlobEndpoint=http://example/devstoreaccount1
        """.trimIndent()

        val remote = nodeConfig(yaml).remotes["az"] as AzureRemote.Factory
        assertTrue(remote.connectionString!!.contains("devstoreaccount1"))
        assertNull(remote.storageAccountKey)
    }

    @Test
    fun `node config decodes !AzureBlob remote with storageAccountKey`() {
        val yaml = """
            remotes:
              az: !AzureBlob
                storageAccountKey: my-key
        """.trimIndent()

        val remote = nodeConfig(yaml).remotes["az"] as AzureRemote.Factory
        assertEquals("my-key", remote.storageAccountKey)
        assertNull(remote.connectionString)
    }
}
