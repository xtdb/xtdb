package xtdb

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase.Companion.openBase
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import xtdb.api.error.Incorrect

class NodeBaseTest {

    @Test
    fun `the tx-op timer is reported before any database has led`() {
        openBase(openMeterRegistry = true).use { base ->
            assertNotNull(
                base.meterRegistry!!.find("tx.op.timer").timer(),
                "an untagged, node-scoped meter, so it outlives every leader term",
            )
        }
    }

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
}
