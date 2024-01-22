package xtdb.api

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import xtdb.api.log.InMemoryLogFactory
import xtdb.api.log.KafkaLogFactory
import xtdb.api.log.LocalLogFactory
import xtdb.api.storage.InMemoryStorageFactory
import xtdb.api.storage.LocalStorageFactory
import xtdb.api.storage.RemoteStorageFactory
import java.nio.file.Paths

class YamlConfigDecoderTest {
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
        """
        val output = nodeConfig(input)
        println(output.toString())
    }

    @Test
    fun testTxlogDecoding() {
        val inMemoryConfig = "txLog: !InMemory"

        Assertions.assertEquals(
            InMemoryLogFactory(),
            nodeConfig(inMemoryConfig).txLog
        )

        val localConfig = """
        txLog: !Local
            path: test-path
        """

        Assertions.assertEquals(
            LocalLogFactory(path= Paths.get("test-path")),
            nodeConfig(localConfig).txLog
        )

        val kafkaConfig = """
        txLog: !Kafka
            bootstrapServers: localhost:9092
            topicName: xtdb_topic
        """

        Assertions.assertEquals(
            KafkaLogFactory(bootstrapServers = "localhost:9092", topicName = "xtdb_topic"),
            nodeConfig(kafkaConfig).txLog
        )
    }

    @Test
    fun testStorageDecoding() {
        val inMemoryConfig = "storage: !InMemory"

        Assertions.assertEquals(
            InMemoryStorageFactory::class,
            nodeConfig(inMemoryConfig).storage::class
        )

        val localConfig = """
        storage: !Local
            path: test-path
        """

        Assertions.assertEquals(
            LocalStorageFactory(path= Paths.get("test-path")),
            nodeConfig(localConfig).storage
        )

        val s3Config = """
        storage: !Remote
            objectStore: !S3
              bucket: xtdb-bucket
              snsTopicArn: example-arn
            localDiskCache: test-path
        """

        Assertions.assertEquals(
            RemoteStorageFactory(
                objectStore = S3ObjectStoreFactory(bucket = "xtdb-bucket", snsTopicArn = "example-arn"),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(s3Config).storage
        )

        val azureConfig = """
        storage: !Remote
            objectStore: !Azure
              storageAccount: storage-account
              container: xtdb-container
              servicebusNamespace: xtdb-service-bus
              servicebusTopicName: xtdb-service-bus-topic
            localDiskCache: test-path
        """

        Assertions.assertEquals(
            RemoteStorageFactory(
                objectStore = AzureObjectStoreFactory(
                    storageAccount = "storage-account",
                    container = "xtdb-container",
                    servicebusNamespace = "xtdb-service-bus",
                    servicebusTopicName = "xtdb-service-bus-topic"
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
              pubsubTopic: xtdb-bucket-topic
            localDiskCache: test-path
        """

        Assertions.assertEquals(
            RemoteStorageFactory(
                objectStore = GoogleCloudObjectStoreFactory(
                    projectId = "xtdb-project",
                    bucket ="xtdb-bucket",
                    pubsubTopic = "xtdb-bucket-topic"
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
        """
        val output = nodeConfig(input)
        Assertions.assertEquals(
            listOf(
                HttpServerModule(port=3001),
                PgwireServerModule(port=5433),
                FlightSqlServerModule(port=9833)
            ),
            output.getModules()
        )
    }
}
