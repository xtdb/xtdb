package xtdb.kafka.connectsrc

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.database.Database
import xtdb.api.error.Incorrect

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
            indexer = DocsIndexer.Factory(table = "analytics.orders"),
        )

        val restored = protoRoundTrip(original)

        assertEquals("my-kafka", restored.remote)
        assertEquals("orders", restored.topic)
        assertEquals("io.confluent.connect.avro.AvroConverter", restored.connectConfig["value.converter"])
        assertEquals("http://schema-registry:8081", restored.connectConfig["value.converter.schema.registry.url"])
        assertEquals("unwrap", restored.connectConfig["transforms"])
        val docs = restored.indexer as DocsIndexer.Factory
        assertEquals("analytics.orders", docs.table)
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
    fun `YAML with an invalid connectConfig fails to decode`() {
        val yaml = """
            externalSource: !KafkaConnect
              remote: my-kafka
              topic: orders
              connectConfig:
                key.converter: org.apache.kafka.connect.storage.StringConverter
                value.converter: org.apache.kafka.connect.json.JsonConverter
                transfroms: mask
              indexer: !Docs
                table: orders
        """.trimIndent()

        assertThrows<Incorrect> { Database.Config.fromYaml(yaml) }
    }

    @Test
    fun `proto round-trips an invalid connectConfig untouched — history is never validated`() {
        val invalid = KafkaConnectSource.Factory(
            remote = "my-kafka",
            topic = "orders",
            connectConfig = mapOf("transfroms" to "mask"),
            indexer = DocsIndexer.Factory(table = "orders"),
        )

        assertEquals("mask", protoRoundTrip(invalid).connectConfig["transfroms"])
    }

    @Test
    fun `connectConfig defaults to empty map`() {
        val factory = KafkaConnectSource.Factory(remote = "k", topic = "t", indexer = DocsIndexer.Factory(table = "orders"))

        assertTrue(factory.connectConfig.isEmpty())
    }

    @Test
    fun `YAML !Docs table decodes verbatim, including a qualified name`() {
        fun tableFor(tableYaml: String): String {
            val yaml = """
                externalSource: !KafkaConnect
                  remote: k
                  topic: t
                  connectConfig:
                    key.converter: org.apache.kafka.connect.storage.StringConverter
                    value.converter: org.apache.kafka.connect.json.JsonConverter
                  indexer: !Docs
                    table: $tableYaml
            """.trimIndent()

            val factory = Database.Config.fromYaml(yaml).externalSource as KafkaConnectSource.Factory
            return (factory.indexer as DocsIndexer.Factory).table
        }

        assertEquals("events", tableFor("events"))
        assertEquals("analytics.events", tableFor("analytics.events"))
    }
}
