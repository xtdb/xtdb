package xtdb.kafka.connectsrc

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.ingestNodeConfig
import xtdb.api.log.KafkaCluster

class IngestNodeConfigTest {

    @Test
    fun `decodes a full ingest-node YAML config`() {
        val yaml = """
            logClusters:
              main-kafka: !Kafka
                bootstrapServers: kafka:9092
            remotes:
              src-kafka: !Kafka
                bootstrapServers: upstream:9092
            databases:
              orders:
                log: !Kafka
                  cluster: main-kafka
                  topic: orders-log
                externalSource: !KafkaConnect
                  remote: src-kafka
                  topic: orders-events
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                  indexer: !Docs
                    table: orders
            nodeId: ingest-1
        """.trimIndent()

        val config = ingestNodeConfig(yaml)

        assertEquals("ingest-1", config.nodeId)
        assertEquals("kafka:9092", (config.logClusters["main-kafka"] as KafkaCluster.ClusterFactory).bootstrapServers)

        val db = config.databases["orders"]!!
        assertEquals("orders-log", (db.log as KafkaCluster.LogFactory).topic)

        val source = db.externalSource as KafkaConnectSource.Factory
        assertEquals("src-kafka", source.remote)
        assertEquals("orders-events", source.topic)
        assertEquals("orders", (source.indexer as DocsIndexer.Factory).table)
    }
}
