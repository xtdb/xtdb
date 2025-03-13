package xtdb.aws

import org.junit.jupiter.api.Tag
import xtdb.api.storage.ObjectStoreTest
import xtdb.aws.S3.Companion.s3
import java.lang.System.getProperty
import java.nio.file.Path

@Tag("s3")
open class S3Test : ObjectStoreTest() {

    companion object {
        @JvmStatic
        protected val bucket: String = getProperty("xtdb.aws.s3-test.bucket") ?: "xtdb-object-store-iam-test"
    }

    override fun openObjectStore(prefix: Path) = s3(bucket) { prefix(prefix) }.openObjectStore()
}