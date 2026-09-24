package xtdb.indexer

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.Relation
import xtdb.test.AllocatorResolver
import xtdb.util.closeAll
import java.time.Instant

@ExtendWith(AllocatorResolver::class)
class ResolvedTxTableDataTest {

    @Test
    fun `a resolved tx's table data survives the replica log as the rows it was written from`(al: BufferAllocator) {
        val msg = Relation(al).use { rel ->
            repeat(10) { rel.writeRow(mapOf("_id" to it.toLong(), "name" to "row-$it")) }

            ReplicaMessage.ResolvedTx(
                txId = 3, systemTime = Instant.EPOCH, committed = true, error = null,
                tableData = mapOf("public/docs" to rel.toArrowStream(4, 3)),
                termId = 1,
            )
        }

        val decoded = ReplicaMessage.decode(msg.encode()) as ReplicaMessage.ResolvedTx
        decoded shouldBe msg

        val tables = decoded.loadTableData(al)
        try {
            tables.values.single()["_id"].asList shouldBe listOf(4L, 5L, 6L)
            tables.values.single()["name"].asList shouldBe listOf("row-4", "row-5", "row-6")
        } finally {
            tables.values.closeAll()
        }
    }
}
