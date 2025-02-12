package xtdb.aws

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.regions.Region
import java.nio.file.Path

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
        region(Region.AWS_ISO_GLOBAL)
        prefix(prefix)
        pathStyleAccessEnabled(true)
    }.openObjectStore()
}