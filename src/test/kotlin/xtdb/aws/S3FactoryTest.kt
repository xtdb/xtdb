package xtdb.aws

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.error.Incorrect
import xtdb.api.nodeConfig
import xtdb.aws.S3.Companion.s3
import xtdb.aws.proto.S3ObjectStoreConfig
import xtdb.util.asPath
import kotlin.io.path.Path

class S3FactoryTest {

    private val errorCodeKey = Keyword.intern("xtdb.error", "code")
    private fun Incorrect.errorCode() = data.valAt(errorCodeKey) as Keyword?

    private class StubRemote : Remote {
        override fun close() = Unit
    }

    @Test
    fun `proto round trip with inline credentials`() {
        @Suppress("DEPRECATION")
        val original = s3("test-bucket") {
            region("us-east-1")
            prefix("test/prefix".asPath)
            credentials("test-access", "test-secret")
            endpoint("https://s3.example.com")
        }

        val restored = S3.Registration().fromProto(original.configProto)
        assertEquals(original, restored)
    }

    @Test
    fun `proto round trip — alias path strips credentials`() {
        // The leak-fix invariant: when the alias path is in use, the proto must
        // not carry access/secret keys — those are node-local and would otherwise
        // land on the source log.
        val original = s3("test-bucket") {
            prefix("test/prefix".asPath)
            remote("aws")
        }

        val proto = original.configProto.unpack(S3ObjectStoreConfig::class.java)
        assertEquals("aws", proto.remote)
        assertFalse(proto.hasCredentials(), "credentials must not appear on alias path")

        val restored = S3.Registration().fromProto(original.configProto)
        assertEquals("aws", restored.remote)
        assertNull(restored.credentials)
    }

    @Test
    fun `openObjectStore errors when both remote and inline creds set`() {
        @Suppress("DEPRECATION")
        val factory = s3("test-bucket") {
            remote("aws")
            credentials("test-access", "test-secret")
        }

        val e = assertThrows<Incorrect> {
            factory.openObjectStore(Path("storage-root"), emptyMap())
        }
        assertEquals(Keyword.intern("xtdb.aws", "conflicting-config"), e.errorCode())
    }

    @Test
    fun `openObjectStore errors on missing remote alias`() {
        val factory = s3("test-bucket") { remote("aws") }

        val e = assertThrows<Incorrect> {
            factory.openObjectStore(Path("storage-root"), emptyMap())
        }
        assertEquals(Keyword.intern("xtdb.aws", "missing-remote"), e.errorCode())
    }

    @Test
    fun `openObjectStore errors on wrong remote type`() {
        val factory = s3("test-bucket") { remote("aws") }

        val remotes: Map<RemoteAlias, Remote> = mapOf("aws" to StubRemote())
        val e = assertThrows<Incorrect> {
            factory.openObjectStore(Path("storage-root"), remotes)
        }
        assertEquals(Keyword.intern("xtdb.aws", "wrong-remote-type"), e.errorCode())
    }

    @Test
    fun `node config decodes !S3 remote under remotes`() {
        val yaml = """
            remotes:
              aws: !S3
                accessKey: my-access
                secretKey: my-secret
        """.trimIndent()

        val remote = nodeConfig(yaml).remotes["aws"] as S3Remote.Factory
        assertEquals("my-access", remote.accessKey)
        assertEquals("my-secret", remote.secretKey)
    }
}
