package xtdb

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import xtdb.api.error.Incorrect

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
    fun `a compacting node without withinFileRecencyBranching refuses to start`() {
        val thrown = assertThrows<Incorrect> {
            Xtdb.openNode { compactor { threads(1); withinFileRecencyBranching(false) } }
        }

        assertTrue(
            thrown.message?.contains("withinFileRecencyBranching") == true,
            "error message should name the setting, was: ${thrown.message}"
        )
    }

    @Test
    fun `a node with its compactor off starts without withinFileRecencyBranching`() {
        Xtdb.openNode { compactor { threads(0); withinFileRecencyBranching(false) } }.close()
    }

    @Test
    fun `a compacting node with withinFileRecencyBranching starts`() {
        Xtdb.openNode { compactor { threads(1); withinFileRecencyBranching(true) } }.close()
    }
}
