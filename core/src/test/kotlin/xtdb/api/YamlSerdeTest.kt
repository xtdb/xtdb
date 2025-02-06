package xtdb.api

import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import xtdb.api.Authenticator.Factory.UserTable
import xtdb.api.Authenticator.Method.PASSWORD
import xtdb.api.Authenticator.Method.TRUST
import xtdb.api.Authenticator.MethodRule
import xtdb.api.log.KafkaLog
import xtdb.api.log.LocalLog.Factory
import xtdb.api.log.Log.Companion.inMemoryLog
import xtdb.api.module.XtdbModule
import xtdb.api.storage.Storage.InMemoryStorageFactory
import xtdb.api.storage.Storage.LocalStorageFactory
import xtdb.api.storage.Storage.RemoteStorageFactory
import xtdb.aws.CloudWatchMetrics
import xtdb.aws.S3.Companion.s3
import xtdb.azure.AzureMonitorMetrics
import xtdb.azure.BlobStorage.Companion.azureBlobStorage
import xtdb.gcp.CloudStorage.Companion.googleCloudStorage
import java.nio.file.Paths

class YamlSerdeTest {

    @Suppress("UNCHECKED_CAST")
    private fun <T : XtdbModule.Factory> Xtdb.Config.findModule(module: Class<out XtdbModule.Factory>): T? =
        getModules().find { module.isAssignableFrom(it::class.java) } as T?

    private inline fun <reified T : XtdbModule.Factory> Xtdb.Config.findModule(): T? = findModule(T::class.java)

    @Test
    fun testDecoder() {
        val input = """
        server: 
            port: 3000
            numThreads: 42
            ssl: 
                keyStore: test-path
                keyStorePassword: password
        log: !InMemory
        storage: !Local
            path: local-storage
            maxCacheEntries: 1025
        indexer:
            logLimit: 65
            flushDuration: PT4H
        healthz:
            port: 3000
        defaultTz: "America/Los_Angeles"
        """.trimIndent()

        println(nodeConfig(input).toString())
    }

    @Test
    fun testMetricsConfigDecoding() {
        val input = """
        healthz: 
          port: 3000
        """.trimIndent()

        assertEquals(3000, nodeConfig(input).healthz?.port)

        val awsInput = """
        modules: 
          - !CloudWatch
            namespace: "aws.namespace" 
        """.trimIndent()

        assertEquals("aws.namespace", nodeConfig(awsInput).findModule<CloudWatchMetrics>()?.namespace)

        val azureInput = """
        modules: 
          - !AzureMonitor
            connectionString: "InstrumentationKey=00000000-0000-0000-0000-000000000000;" 
        """.trimIndent()

        assertEquals("InstrumentationKey=00000000-0000-0000-0000-000000000000;", nodeConfig(azureInput).findModule<AzureMonitorMetrics>()?.connectionString)
    }

    @Test
    fun testLogDecoding() {
        val inMemoryConfig = "log: !InMemory"

        assertEquals(inMemoryLog, nodeConfig(inMemoryConfig).log)

        val localConfig = """
        log: !Local
            path: test-path
        """.trimIndent()

        assertEquals(Factory(path = Paths.get("test-path")), nodeConfig(localConfig).log)

        val kafkaConfig = """
        log: !Kafka
            bootstrapServers: localhost:9092
            topic: xtdb_topic
            
        """.trimIndent()

        assertEquals(
            KafkaLog.Factory(bootstrapServers = "localhost:9092", topic = "xtdb_topic"),
            nodeConfig(kafkaConfig).log
        )
    }

    @Test
    fun testStorageDecoding() {
        mockkObject(EnvironmentVariableProvider)

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

        every { EnvironmentVariableProvider.getEnvVariable("S3_BUCKET") } returns "xtdb-bucket"

        val s3Config = """
        storage: !Remote
            objectStore: !S3
              bucket: !Env S3_BUCKET
              prefix: xtdb-object-store
            localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = s3(bucket = "xtdb-bucket").prefix(Paths.get("xtdb-object-store")),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(s3Config).storage
        )

        every { EnvironmentVariableProvider.getEnvVariable("AZURE_STORAGE_ACCOUNT") } returns "storage-account"

        val azureConfig = """
        storage: !Remote
            objectStore: !Azure
              storageAccount: !Env AZURE_STORAGE_ACCOUNT
              container: xtdb-container
              prefix: xtdb-object-store
            localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = azureBlobStorage(
                    storageAccount = "storage-account",
                    container = "xtdb-container"
                ).prefix(Paths.get("xtdb-object-store")),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(azureConfig).storage
        )

        every { EnvironmentVariableProvider.getEnvVariable("GCP_PROJECT_ID") } returns "xtdb-project"

        val googleCloudConfig = """
        storage: !Remote
          objectStore: !GoogleCloud
            projectId: !Env GCP_PROJECT_ID
            bucket: xtdb-bucket
            prefix: xtdb-object-store
          localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = googleCloudStorage(
                    projectId = "xtdb-project",
                    bucket ="xtdb-bucket",
                ).prefix(Paths.get("xtdb-object-store")),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(googleCloudConfig).storage
        )

        unmockkObject(EnvironmentVariableProvider)
    }

    @Test
    fun testModuleDecoding() {
        val input = """
        modules:
            - !HttpServer
              port: 3001
            - !FlightSqlServer
              port: 9833
        """.trimIndent()

        assertEquals(
            listOf(
                HttpServer.Factory(port = 3001),
                FlightSqlServer.Factory(port = 9833)
            ),
            nodeConfig(input).getModules()
        )
    }

    @Test
    fun testEnvVarsWithUnsetVariable() {
        val inputWithEnv = """
        log: !Local
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
        log: !Local
            path: !Env TX_LOG_PATH
        """.trimIndent()

        assertEquals(
            Factory(path = Paths.get("test-path")),
            nodeConfig(inputWithEnv).log
        )

        unmockkObject(EnvironmentVariableProvider)
    }

    @Test
    fun testEnvVarsMultipleSetVariables() {
        mockkObject(EnvironmentVariableProvider)
        every { EnvironmentVariableProvider.getEnvVariable("BUCKET") } returns "xtdb-bucket"
        every { EnvironmentVariableProvider.getEnvVariable("DISK_CACHE_PATH") } returns "test-path"

        val inputWithEnv = """
        storage: !Remote
            objectStore: !S3
              bucket: !Env BUCKET 
            localDiskCache: !Env DISK_CACHE_PATH
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = s3(bucket = "xtdb-bucket"),
                localDiskCache = Paths.get("test-path")
            ),

            nodeConfig(inputWithEnv).storage
        )

        unmockkObject(EnvironmentVariableProvider)
    }

    @Test
    fun testNestedEnvVarInMaps() {
        mockkObject(EnvironmentVariableProvider)
        every { EnvironmentVariableProvider.getEnvVariable("KAFKA_BOOTSTRAP_SERVERS") } returns "localhost:9092"
        val saslConfig =
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"password\";"
        every { EnvironmentVariableProvider.getEnvVariable("KAFKA_SASL_JAAS_CONFIG") } returns saslConfig

        val inputWithEnv = """
        log: !Kafka
            bootstrapServers: !Env KAFKA_BOOTSTRAP_SERVERS
            topic: xtdb_topic
            propertiesMap:
                security.protocol: SASL_SSL
                sasl.mechanism: PLAIN
                sasl.jaas.config: !Env KAFKA_SASL_JAAS_CONFIG
        """.trimIndent()

        assertEquals(
            KafkaLog.Factory(
                bootstrapServers = "localhost:9092",
                topic = "xtdb_topic",
                propertiesMap = mapOf(
                    "security.protocol" to "SASL_SSL",
                    "sasl.mechanism" to "PLAIN",
                    "sasl.jaas.config" to saslConfig
                )
            ),
            nodeConfig(inputWithEnv).log
        )

        unmockkObject(EnvironmentVariableProvider)
    }

    @Test
    fun testPortSetWithEnvVar() {
        mockkObject(EnvironmentVariableProvider)
        every { EnvironmentVariableProvider.getEnvVariable("HEALTHZ_PORT") } returns "8081"

        val input = """
        healthz: 
          port: !Env HEALTHZ_PORT
        """.trimIndent()

        assertEquals(8081, nodeConfig(input).healthz?.port)

        unmockkObject(EnvironmentVariableProvider)
    }

    @Test
    fun testAuthnConfigDecoding() {
        val input = """
        authn: !UserTable
          rules:
              - user: admin
                remoteAddress: 127.0.0.42
                method: TRUST  
              - remoteAddress: 127.0.0.1
                method: PASSWORD  
        """.trimIndent()

        assertEquals(
            UserTable(
                listOf(
                    MethodRule(TRUST, user = "admin", remoteAddress = "127.0.0.42"),
                    MethodRule(PASSWORD, remoteAddress = "127.0.0.1")
                )
            ),
            nodeConfig(input).authn
        )
    }
}
