package xtdb.kafka.connectsrc

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.database.Database

class KafkaConnectSourceFactoryTest {

    private fun protoRoundTrip(factory: KafkaConnectSource.Factory): KafkaConnectSource.Factory {
        val dbConfig = Database.Config().externalSource(factory)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalSource as KafkaConnectSource.Factory
    }

    @Test
    fun `proto round-trips factory`() {
        val original = KafkaConnectSource.Factory(
            remote = "my-kafka",
            topic = "orders",
            connectConfig = mapOf(
                "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
                "value.converter" to "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url" to "http://schema-registry:8081",
                "transforms" to "unwrap",
                "transforms.unwrap.type" to "org.apache.kafka.connect.transforms.ExtractField\$Value",
                "transforms.unwrap.field" to "payload",
            ),
            indexer = DocsIndexer.Factory(table = "orders"),
        )

        val restored = protoRoundTrip(original)

        assertEquals("my-kafka", restored.remote)
        assertEquals("orders", restored.topic)
        assertEquals("io.confluent.connect.avro.AvroConverter", restored.connectConfig["value.converter"])
        assertEquals("http://schema-registry:8081", restored.connectConfig["value.converter.schema.registry.url"])
        assertEquals("unwrap", restored.connectConfig["transforms"])
        val docs = restored.indexer as DocsIndexer.Factory
        assertEquals("orders", docs.table)
    }

    @Test
    fun `YAML round-trips !KafkaConnect with !Docs indexer`() {
        val yaml = """
            externalSource: !KafkaConnect
              remote: my-kafka
              topic: orders
              connectConfig:
                key.converter: org.apache.kafka.connect.storage.StringConverter
                value.converter: org.apache.kafka.connect.json.JsonConverter
                value.converter.schemas.enable: "false"
              indexer: !Docs
                table: orders
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val factory = config.externalSource as KafkaConnectSource.Factory

        assertEquals("my-kafka", factory.remote)
        assertEquals("orders", factory.topic)
        assertEquals("org.apache.kafka.connect.json.JsonConverter", factory.connectConfig["value.converter"])
        assertEquals("false", factory.connectConfig["value.converter.schemas.enable"])
        val docs = factory.indexer as DocsIndexer.Factory
        assertEquals("orders", docs.table)
    }

    @Test
    fun `connectConfig defaults to empty map`() {
        val yaml = """
            externalSource: !KafkaConnect
              remote: k
              topic: t
              indexer: !Docs
                table: orders
        """.trimIndent()

        val factory = Database.Config.fromYaml(yaml).externalSource as KafkaConnectSource.Factory

        assertTrue(factory.connectConfig.isEmpty())
    }

    @Test
    fun `qualified table name parses to schema + table`() {
        val yaml = """
            externalSource: !KafkaConnect
              remote: k
              topic: t
              indexer: !Docs
                table: analytics.events
        """.trimIndent()

        val factory = Database.Config.fromYaml(yaml).externalSource as KafkaConnectSource.Factory
        val docs = factory.indexer as DocsIndexer.Factory

        assertEquals("analytics.events", docs.table)
    }
}
