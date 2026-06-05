package xtdb.aws

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.api.storage.ObjectStoreTest
import xtdb.aws.S3.Companion.s3
import xtdb.util.asPath
import java.lang.System.getProperty
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Tag("s3")
open class S3Test : ObjectStoreTest() {

    companion object {
        @JvmStatic
        protected val bucket: String = getProperty("xtdb.aws.s3-test.bucket") ?: "xtdb-object-store-iam-test"
    }

    override fun openObjectStore(prefix: Path) =
        s3(bucket).prefix(prefix).openObjectStore(Path("s3-test"))

    @Test
    fun `test proto round trip`() = runTest {
        val originalFactory = s3("test-bucket") {
            region("us-west-2")
            prefix("test/prefix".asPath)
            credentials("access-key", "secret-key")
            endpoint("http://localhost:9000")
            pathStyleAccessEnabled(true)
        }

        val registration = S3.Registration()
        val proto = originalFactory.configProto
        val deserializedFactory = registration.fromProto(proto)

        assertEquals(originalFactory, deserializedFactory)
    }

    @Test
    fun `proto round trip without credentials leaves the default chain to resolve them`() = runTest {
        val deserializedFactory = S3.Registration().fromProto(s3("test-bucket").configProto) as S3.Factory

        assertNull(deserializedFactory.credentials)
    }
}