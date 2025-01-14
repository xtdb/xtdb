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
import xtdb.api.log.Kafka
import xtdb.api.log.Logs.InMemoryLogFactory
import xtdb.api.log.Logs.LocalLogFactory
import xtdb.api.module.XtdbModule
import xtdb.api.storage.AzureBlobStorage.azureBlobStorage
import xtdb.api.storage.GoogleCloudStorage
import xtdb.api.storage.Storage.InMemoryStorageFactory
import xtdb.api.storage.Storage.LocalStorageFactory
import xtdb.api.storage.Storage.RemoteStorageFactory
import xtdb.aws.CloudWatchMetrics
import xtdb.aws.S3.s3
import xtdb.azure.AzureMonitorMetrics
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
        txLog: !InMemory
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
            txTopic: xtdb_tx_topic
            filesTopic: xdtd_files_topic
            
        """.trimIndent()

        assertEquals(
            Kafka.Factory(bootstrapServers = "localhost:9092", txTopic = "xtdb_tx_topic", filesTopic = "xdtd_files_topic"),
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
            localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = s3(bucket = "xtdb-bucket"),
                localDiskCache = Paths.get("test-path")
            ),
            nodeConfig(s3Config).storage
        )

        val azureConfig = """
        storage: !Remote
            objectStore: !Azure
              storageAccount: storage-account
              container: xtdb-container
            localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = azureBlobStorage(
                    storageAccount = "storage-account",
                    container = "xtdb-container"
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
          localDiskCache: test-path
        """.trimIndent()

        assertEquals(
            RemoteStorageFactory(
                objectStore = GoogleCloudStorage.Factory(
                    projectId = "xtdb-project",
                    bucket ="xtdb-bucket"
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
        txLog: !Kafka
            bootstrapServers: !Env KAFKA_BOOTSTRAP_SERVERS
            txTopic: xtdb_tx_topic
            filesTopic: xdtd_files_topic
            propertiesMap:
                security.protocol: SASL_SSL
                sasl.mechanism: PLAIN
                sasl.jaas.config: !Env KAFKA_SASL_JAAS_CONFIG
        """.trimIndent()

        assertEquals(
            Kafka.Factory(
                bootstrapServers = "localhost:9092",
                txTopic = "xtdb_tx_topic",
                filesTopic = "xdtd_files_topic",
                propertiesMap = mapOf(
                    "security.protocol" to "SASL_SSL",
                    "sasl.mechanism" to "PLAIN",
                    "sasl.jaas.config" to saslConfig
                )
            ),
            nodeConfig(inputWithEnv).txLog
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
