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
            )
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
              log: !Kafka
                logCluster: my-cluster
                tableTopic: cdc.public.users
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val source = config.externalLog as DebeziumSource
        val log = source.log as KafkaDebeziumLog.Factory

        assertEquals("my-cluster", log.logCluster)
        assertEquals("cdc.public.users", log.tableTopic)
    }

    @Test
    fun `YAML parses Database Config without external source`() {
        val config = Database.Config.fromYaml("mode: read-write")
        assertNull(config.externalLog)
    }

    @Test
    fun `external source is nullable in Config`() {
        val config = Database.Config()
        assertNull(config.externalLog)

        val withSource = config.externalSource(DebeziumSource(
            log = KafkaDebeziumLog.Factory("cluster", "topic")
        ))
        assertNotNull(withSource.externalLog)

        val withoutSource = withSource.externalSource(null)
        assertNull(withoutSource.externalLog)
    }
}
