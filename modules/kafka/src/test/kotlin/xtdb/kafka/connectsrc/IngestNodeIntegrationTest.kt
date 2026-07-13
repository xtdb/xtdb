package xtdb.kafka.connectsrc

import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import xtdb.api.IngestNode
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * End-to-end test of the embedding story: a custom JVM app builds an [IngestNode.Config] with a
 * [KafkaConnectSource.Factory] wrapping its own [RecordIndexer] — a plain, non-serialisable
 * factory, no Registration — and runs it as an ingest-only node. We assert the indexer sees the
 * records, proving the programmatic path works through the real Kafka consumer + ingest node.
 */
@Tag("integration")
class IngestNodeIntegrationTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            Startables.deepStart(kafka).join()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            kafka.stop()
            network.close()
        }
    }

    private fun createTopic(topic: String) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }
    }

    private fun produce(topic: String, key: String, value: ByteArray) {
        val props = mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to StringSerializer::class.java.name,
            "value.serializer" to ByteArraySerializer::class.java.name,
        )
        KafkaProducer<String, ByteArray>(props).use { it.send(ProducerRecord(topic, key, value)).get() }
    }

    private suspend fun awaitCondition(description: String, timeout: Duration = 30.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (System.currentTimeMillis() < deadline) {
            if (runCatching(check).getOrDefault(false)) return
            runInterruptible { Thread.sleep(200) }
        }
        throw AssertionError("Timed out waiting for: $description")
    }

    /** A user-supplied indexer that records the keys it sees rather than writing them anywhere. */
    private class CapturingIndexer(val seenKeys: ConcurrentLinkedQueue<String>) : RecordIndexer {
        class Factory(val seenKeys: ConcurrentLinkedQueue<String>) : RecordIndexer.Factory {
            override fun open(): RecordIndexer = CapturingIndexer(seenKeys)
        }

        override suspend fun indexRecords(records: List<SinkRecord>, txIndexer: TxIndexer) {
            for (rec in records) {
                txIndexer.executeTx(externalSourceToken = null) {
                    seenKeys.add(rec.key() as String)
                    TxResult.Committed()
                }
            }
        }
    }

    @Test
    fun `programmatic indexer ingests through an ingest node`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produce(sourceTopic, "k1", """{"name":"Alice"}""".toByteArray())
        produce(sourceTopic, "k2", """{"name":"Bob"}""".toByteArray())

        val seenKeys = ConcurrentLinkedQueue<String>()

        val dbConfig = Database.Config(
            log = KafkaCluster.LogFactory("kafka", "replica-${UUID.randomUUID()}"),
            storage = Storage.inMemory(),
            externalSource = KafkaConnectSource.Factory(
                remote = "kafka",
                topic = sourceTopic,
                connectConfig = mapOf(
                    "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable" to "false",
                ),
                indexer = CapturingIndexer.Factory(seenKeys),
            ),
        )

        IngestNode.Config()
            .remote("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
            .database("events", dbConfig)
            .open()
            .use {
                awaitCondition("both records indexed") { seenKeys.toSet() == setOf("k1", "k2") }

                produce(sourceTopic, "k3", """{"name":"Charlie"}""".toByteArray())
                awaitCondition("streamed record indexed") { "k3" in seenKeys }
            }

        assertEquals(listOf("k1", "k2", "k3"), seenKeys.toList())
    }

    private fun attach(node: Xtdb, dbName: String, yaml: String) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("ATTACH DATABASE $dbName WITH \$\$\n$yaml\n\$\$")
            }
        }
    }

    private fun queryIds(node: Xtdb): Set<String> =
        node.createConnectionBuilder().database("events").build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT _id FROM public.events").use { rs ->
                    buildSet { while (rs.next()) add(rs.getString("_id")) }
                }
            }
        }

    @Test
    fun `read-only attach sees ingest-node data before and after a block flush`(@TempDir storageDir: Path) =
        runTest(timeout = 180.seconds) {
            val sourceTopic = "events-${UUID.randomUUID()}"
            val logTopic = "xt-log-${UUID.randomUUID()}"
            createTopic(sourceTopic)

            produce(sourceTopic, "k1", """{"name":"Alice"}""".toByteArray())
            produce(sourceTopic, "k2", """{"name":"Bob"}""".toByteArray())

            val dbConfig = Database.Config(
                log = KafkaCluster.LogFactory("kafka", logTopic),
                storage = Storage.local(storageDir),
                externalSource = KafkaConnectSource.Factory(
                    remote = "kafka",
                    topic = sourceTopic,
                    connectConfig = mapOf(
                        "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
                        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter.schemas.enable" to "false",
                    ),
                    indexer = DocsIndexer.Factory("events"),
                ),
            )

            IngestNode.Config()
                .remote("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
                .database("events", dbConfig)
                .open()
                .use { ingestNode ->
                    Xtdb.openNode {
                        server { port = 0 }
                        flightSql = null
                        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
                    }.use { queryNode ->
                        attach(queryNode, "events", """
                            log: !Kafka
                              cluster: kafka
                              topic: $logTopic
                            storage: !Local
                              path: '$storageDir'
                            mode: read-only
                        """.trimIndent())

                        awaitCondition("read-only attach sees pre-flush records") {
                            queryIds(queryNode) == setOf("k1", "k2")
                        }

                        val ingestDb = checkNotNull(ingestNode.database("events"))
                        ingestDb.sendFlushBlockMessage()
                        awaitCondition("ingest node cuts the block") {
                            ingestDb.blockCatalog.currentBlockIndex == 0L
                        }

                        val followerDb = (queryNode as Xtdb.XtdbInternal).dbCatalog["events"]
                        awaitCondition("read-only attach processes the flushed block") {
                            followerDb?.blockCatalog?.currentBlockIndex == 0L
                        }

                        assertEquals(
                            setOf("k1", "k2"), queryIds(queryNode),
                            "pre-flush records still readable from the flushed block",
                        )

                        produce(sourceTopic, "k3", """{"name":"Charlie"}""".toByteArray())
                        awaitCondition("read-only attach sees the post-flush record") {
                            queryIds(queryNode) == setOf("k1", "k2", "k3")
                        }
                    }
                }
        }
}
