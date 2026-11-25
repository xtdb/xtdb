package xtdb

import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import xtdb.api.error.Incorrect
import java.lang.ref.WeakReference

class NodeBaseTest {

    @Test
    fun `same alias under logClusters and remotes fails node startup`() {
        val thrown = assertThrows<Incorrect> {
            Xtdb.openNode {
                logCluster("conflict", KafkaCluster.ClusterFactory("localhost:9092"))
                remote("conflict", KafkaCluster.ClusterFactory("localhost:9093"))
            }
        }

        assertTrue(
            thrown.message?.contains("conflict") == true,
            "error message should reference the offending alias, was: ${thrown.message}"
        )
    }

    @Test
    fun `a closed node base's meter registry can be garbage collected`() {
        val registry = NodeBase.openBase().use { WeakReference(it.meterRegistry!!) }

        repeat(20) {
            if (registry.get() == null) return@repeat
            System.gc()
            Thread.sleep(50)
        }

        assertNull(registry.get(), "nothing should hold the registry once its node base is closed")
    }
}
