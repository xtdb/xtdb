package xtdb.api

import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import xtdb.api.log.Kafka
import xtdb.api.log.Logs.InMemoryLogFactory
import xtdb.api.log.Logs.LocalLogFactory
import xtdb.api.storage.AzureBlobStorage.azureBlobStorage
import xtdb.api.storage.GoogleCloudStorage
import xtdb.api.storage.S3.s3
import xtdb.api.storage.Storage.InMemoryStorageFactory
import xtdb.api.storage.Storage.LocalStorageFactory
import xtdb.api.storage.Storage.RemoteStorageFactory
import java.nio.file.Paths

class YamlSerdeTest {
    @Test
    fun testDecoder() {
        val input = """
        defaultTz: "America/Los_Angeles"
        txLog: !InMemory
        indexer:
            logLimit: 65
            flushDuration: PT4H
        storage: !Local
            path: local-storage
            maxCacheEntries: 1025
        """.trimIndent()

        println(nodeConfig(input).toString())
    }

    @Test
    fun testTxLogDecoding() {
        val inMemoryConfig = "txLog: !InMemory"

        assertEquals(InMemoryLogFactory(), nodeConfig(inMemoryConfig).txLog)

        val localConfig = """
        txLog: !Local
            path: test-path
        """.trimIndent()

        assertEquals(LocalLogFactory(path = Paths.get("test-path")), nodeConfig(localConfig).txLog)

        val kafkaConfig = """
        txLog: !Kafka
            bootstrapServers: localhost:9092
            topicName: xtdb_topic
        """.trimIndent()

        assertEquals(
            Kafka.Factory(bootstrapServers = "localhost:9092", topicName = "xtdb_topic"),
            nodeConfig(kafkaConfig).txLog
        )
    }

    @Test
    fun testStorageDecoding() {
        val inMemoryConfig = "storage: !InMemory"

        assertEquals(
            InMemoryStorageFactory::class,
            nodeConfig(inMemoryConfig).storage::class
        )

        val localConfig = """
        storage: !Local
            path: test-path
        """.trimIndent()

        assertEquals(
            LocalStorageFactory(path = Paths.get("test-path")),
            nodeConfig(localConfig).storage
        )

        val s3Config = """
        storage: !Remote
            objectStore: !S3
              bucket: xtdb-bucket
              snsTopicArn: example-arn
            localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = s3(bucket = "xtdb-bucket", snsTopicArn = "example-arn"),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(s3Config).storage
        )

        val azureConfig = """
        storage: !Remote
            objectStore: !Azure
              storageAccount: storage-account
              container: xtdb-container
              serviceBusNamespace: xtdb-service-bus
              serviceBusTopicName: xtdb-service-bus-topic
            localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = azureBlobStorage(
                    storageAccount = "storage-account",
                    container = "xtdb-container",
                    serviceBusNamespace = "xtdb-service-bus",
                    serviceBusTopicName = "xtdb-service-bus-topic"
                ),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(azureConfig).storage
        )

        val googleCloudConfig = """
        storage: !Remote
          objectStore: !GoogleCloud
            projectId: xtdb-project
            bucket: xtdb-bucket
            pubSubTopic: xtdb-bucket-topic
          localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = GoogleCloudStorage.Factory(
                    projectId = "xtdb-project",
                    bucket ="xtdb-bucket",
                    pubSubTopic = "xtdb-bucket-topic"
                ),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(googleCloudConfig).storage
        )
    }

    @Test
    fun testModuleDecoding() {
        val input = """
        modules:
            - !HttpServer
              port: 3001
            - !PgwireServer
              port: 5433
            - !FlightSqlServer
              port: 9833
        """.trimIndent()

        assertEquals(
            listOf(
                HttpServer.Factory(port = 3001),
                PgwireServer.Factory(port = 5433),
                FlightSqlServer.Factory(port = 9833)
            ),
            nodeConfig(input).getModules()
        )
    }

    @Test
    fun testEnvVarsWithUnsetVariable() {
        val inputWithEnv = """
        txLog: !Local
            path: !Env TX_LOG_PATH
        """.trimIndent()

        val thrown = assertThrows(IllegalArgumentException::class.java) {
            nodeConfig(inputWithEnv)
        }

        assertEquals("Environment variable 'TX_LOG_PATH' not found", thrown.message)
    }

    @Test
    fun testEnvVarsWithSetVariable() {
        mockkObject(EnvironmentVariableProvider)
        every { EnvironmentVariableProvider.getEnvVariable("TX_LOG_PATH") } returns "test-path"

        val inputWithEnv = """
        txLog: !Local
            path: !Env TX_LOG_PATH
        """.trimIndent()

        assertEquals(
            LocalLogFactory(path = Paths.get("test-path")),
            nodeConfig(inputWithEnv).txLog
        )

        unmockkObject(EnvironmentVariableProvider)
    }

    @Test
    fun testEnvVarsMultipleSetVariables() {
        mockkObject(EnvironmentVariableProvider)
        every { EnvironmentVariableProvider.getEnvVariable("BUCKET") } returns "xtdb-bucket"
        every { EnvironmentVariableProvider.getEnvVariable("SNS_TOPIC_ARN") } returns "example-arn"
        every { EnvironmentVariableProvider.getEnvVariable("DISK_CACHE_PATH") } returns "test-path"

        val inputWithEnv = """
        storage: !Remote
            objectStore: !S3
              bucket: !Env BUCKET 
              snsTopicArn: !Env SNS_TOPIC_ARN
            localDiskCache: !Env DISK_CACHE_PATH 
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = s3(bucket = "xtdb-bucket", snsTopicArn = "example-arn"),
                localDiskCache = Paths.get("test-path")
            ),

            nodeConfig(inputWithEnv).storage
        )

        unmockkObject(EnvironmentVariableProvider)
    }
}
