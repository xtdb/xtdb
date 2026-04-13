package xtdb.debezium

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.database.Database

class DebeziumKafkaSourceFactoryTest {

    private fun protoRoundTrip(factory: DebeziumKafkaSource.Factory): DebeziumKafkaSource.Factory {
        val dbConfig = Database.Config().externalSource(factory)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalSource as DebeziumKafkaSource.Factory
    }

    @Test
    fun `proto round-trips factory`() {
        val original = DebeziumKafkaSource.Factory(
            logCluster = "my-cluster",
            tableTopic = "cdc.public.users",
            messageFormat = MessageFormat.Json,
        )

        val restored = protoRoundTrip(original)

        assertEquals("my-cluster", restored.logCluster)
        assertEquals("cdc.public.users", restored.tableTopic)
        assertEquals(MessageFormat.Json, restored.messageFormat)
    }

    @Test
    fun `proto round-trips Database Config without external source`() {
        val dbConfig = Database.Config()
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)

        assertNull(restored.externalSource)
    }

    @Test
    fun `YAML round-trips factory`() {
        val yaml = """
            externalSource: !DebeziumKafka
              logCluster: my-cluster
              tableTopic: cdc.public.users
              messageFormat: !Json {}
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val factory = config.externalSource as DebeziumKafkaSource.Factory

        assertEquals("my-cluster", factory.logCluster)
        assertEquals("cdc.public.users", factory.tableTopic)
        assertEquals(MessageFormat.Json, factory.messageFormat)
    }

    @Test
    fun `YAML parses Database Config without external source`() {
        val config = Database.Config.fromYaml("mode: read-write")
        assertNull(config.externalSource)
    }

    @Test
    fun `proto round-trips Avro message format`() {
        val original = DebeziumKafkaSource.Factory(
            logCluster = "my-cluster",
            tableTopic = "cdc.public.users",
            messageFormat = MessageFormat.Avro,
        )

        val restored = protoRoundTrip(original)
        assertEquals(MessageFormat.Avro, restored.messageFormat)
    }

    @Test
    fun `YAML round-trips Avro message format`() {
        val yaml = """
            externalSource: !DebeziumKafka
              logCluster: my-cluster
              tableTopic: cdc.public.users
              messageFormat: !Avro {}
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val factory = config.externalSource as DebeziumKafkaSource.Factory
        assertEquals(MessageFormat.Avro, factory.messageFormat)
    }

    @Test
    fun `YAML requires messageFormat`() {
        val yaml = """
            externalSource: !DebeziumKafka
              logCluster: my-cluster
              tableTopic: cdc.public.users
        """.trimIndent()

        assertThrows(Exception::class.java) {
            Database.Config.fromYaml(yaml)
        }
    }

    @Test
    fun `external source is nullable in Config`() {
        val config = Database.Config()
        assertNull(config.externalSource)

        val withSource = config.externalSource(DebeziumKafkaSource.Factory(
            logCluster = "cluster",
            tableTopic = "topic",
            messageFormat = MessageFormat.Json,
        ))
        assertNotNull(withSource.externalSource)

        val withoutSource = withSource.externalSource(null)
        assertNull(withoutSource.externalSource)
    }
}
