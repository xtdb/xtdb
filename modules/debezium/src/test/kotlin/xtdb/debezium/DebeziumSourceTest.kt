package xtdb.debezium

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.database.Database

class DebeziumSourceTest {

    private fun protoRoundTrip(source: DebeziumSource): DebeziumSource {
        val dbConfig = Database.Config().externalSource(source)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalLog as DebeziumSource
    }

    @Test
    fun `proto round-trips DebeziumSource with KafkaDebeziumLog`() {
        val original = DebeziumSource(
            log = KafkaDebeziumLog.Factory(
                logCluster = "my-cluster",
                tableTopic = "cdc.public.users",
            ),
            messageFormat = MessageFormat.Json,
        )

        val restored = protoRoundTrip(original)
        val restoredLog = restored.log as KafkaDebeziumLog.Factory

        assertEquals("my-cluster", restoredLog.logCluster)
        assertEquals("cdc.public.users", restoredLog.tableTopic)
    }

    @Test
    fun `proto round-trips Database Config without external source`() {
        val dbConfig = Database.Config()
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)

        assertNull(restored.externalLog)
    }

    @Test
    fun `YAML round-trips DebeziumSource with KafkaDebeziumLog`() {
        val yaml = """
            externalLog: !Debezium
              messageFormat: !Json {}
              log: !Kafka
                logCluster: my-cluster
                tableTopic: cdc.public.users
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val source = config.externalLog as DebeziumSource
        val log = source.log as KafkaDebeziumLog.Factory

        assertEquals("my-cluster", log.logCluster)
        assertEquals("cdc.public.users", log.tableTopic)
        assertEquals(MessageFormat.Json, source.messageFormat)
    }

    @Test
    fun `YAML parses Database Config without external source`() {
        val config = Database.Config.fromYaml("mode: read-write")
        assertNull(config.externalLog)
    }

    @Test
    fun `proto round-trips DebeziumSource with Avro message format`() {
        val original = DebeziumSource(
            log = KafkaDebeziumLog.Factory(
                logCluster = "my-cluster",
                tableTopic = "cdc.public.users",
            ),
            messageFormat = MessageFormat.Avro,
        )

        val restored = protoRoundTrip(original)
        assertEquals(MessageFormat.Avro, restored.messageFormat)
    }

    @Test
    fun `proto round-trips DebeziumSource with Json message format`() {
        val original = DebeziumSource(
            log = KafkaDebeziumLog.Factory(
                logCluster = "my-cluster",
                tableTopic = "cdc.public.users",
            ),
            messageFormat = MessageFormat.Json,
        )

        val restored = protoRoundTrip(original)
        assertEquals(MessageFormat.Json, restored.messageFormat)
    }

    @Test
    fun `YAML round-trips DebeziumSource with Avro message format`() {
        val yaml = """
            externalLog: !Debezium
              messageFormat: !Avro {}
              log: !Kafka
                logCluster: my-cluster
                tableTopic: cdc.public.users
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val source = config.externalLog as DebeziumSource
        assertEquals(MessageFormat.Avro, source.messageFormat)
    }

    @Test
    fun `YAML requires messageFormat`() {
        val yaml = """
            externalLog: !Debezium
              log: !Kafka
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
        assertNull(config.externalLog)

        val withSource = config.externalSource(DebeziumSource(
            log = KafkaDebeziumLog.Factory("cluster", "topic"),
            messageFormat = MessageFormat.Json,
        ))
        assertNotNull(withSource.externalLog)

        val withoutSource = withSource.externalSource(null)
        assertNull(withoutSource.externalLog)
    }
}
