package xtdb.api.log

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.util.asPath

class KafkaLogFactoryTest {

    private fun roundTrip(factory: KafkaCluster.LogFactory): KafkaCluster.LogFactory {
        val dbConfig = Database.Config(factory, Storage.local("storage".asPath))
        return Database.Config.fromProto(dbConfig.serializedConfig).log as KafkaCluster.LogFactory
    }

    @Test
    fun `round-trips default config`() {
        val original = KafkaCluster.LogFactory("myCluster", "my-topic")
        val restored = roundTrip(original)

        assertEquals("my-topic", restored.topic)
        assertEquals("myCluster", restored.cluster)
        assertEquals("my-topic-replica", restored.replicaTopic)
        assertEquals("myCluster", restored.replicaCluster)
        assertEquals(0, restored.epoch)
    }

    @Test
    fun `round-trips custom replica topic`() {
        val original = KafkaCluster.LogFactory("myCluster", "my-topic")
            .replicaTopic("my-custom-replica")

        val restored = roundTrip(original)

        assertEquals("my-custom-replica", restored.replicaTopic)
    }

    @Test
    fun `round-trips custom replica cluster`() {
        val original = KafkaCluster.LogFactory("myCluster", "my-topic")
            .replicaCluster("otherCluster")

        val restored = roundTrip(original)

        assertEquals("otherCluster", restored.replicaCluster)
    }

    @Test
    fun `round-trips groupId`() {
        val original = KafkaCluster.LogFactory("myCluster", "my-topic")
            .groupId("my-group")

        val restored = roundTrip(original)

        assertEquals("my-group", restored.groupId)
    }

    @Test
    fun `round-trips epoch`() {
        val original = KafkaCluster.LogFactory("myCluster", "my-topic")
            .epoch(42)

        val restored = roundTrip(original)

        assertEquals(42, restored.epoch)
    }
}
