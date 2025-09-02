package xtdb.gcp

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.gcp.CloudStorage.Companion.googleCloudStorage
import xtdb.util.asPath

class CloudStorageTest {

    @Test
    fun `test proto round trip`() {
        val originalFactory = googleCloudStorage("test-project", "test-bucket") {
            prefix("test/prefix".asPath)
        }

        val registration = CloudStorage.Registration()
        val proto = originalFactory.configProto
        val deserializedFactory = registration.fromProto(proto)

        assertEquals(originalFactory, deserializedFactory)
    }
}