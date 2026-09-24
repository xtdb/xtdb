package xtdb.api.log

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.catalog.TableCatalog
import xtdb.storage.BufferPool
import java.time.Instant

class ReplicaMessageTermSeqTest {

    private fun roundTrip(msg: ReplicaMessage) = ReplicaMessage.decode(msg.encode())

    @Test
    fun `every message kind carries its term position through the codec`() {
        val msgs = listOf(
            ReplicaMessage.ResolvedTx(1, Instant.EPOCH, true, null, emptyMap(), srcMsgId = 1, termId = 3),
            ReplicaMessage.TriesAdded(1, 0, emptyList(), sourceMsgId = 1, termId = 3),
            ReplicaMessage.BlockBoundary(0, 1, termId = 3),
            ReplicaMessage.BlockUploaded(1, 0, 0, 1, emptyList(), termId = 3),
            ReplicaMessage.NoOp(termId = 3),
            ReplicaMessage.TriesDeleted("public/foo", setOf("trie"), termId = 3),
            ReplicaMessage.OversizedMessage(1, 0, 0, "payload", termId = 3),
        )

        for (msg in msgs) {
            val stamped = msg.withTermSeq(5)
            assertEquals(stamped, roundTrip(stamped))
            assertEquals(5L, roundTrip(stamped)?.termSeq)
        }
    }

    @Test
    fun `a claim's position of zero reads back as zero, distinct from a record with none`() {
        assertEquals(0L, roundTrip(ReplicaMessage.NoOp(termId = 3, termSeq = 0))?.termSeq)
        assertNull(roundTrip(ReplicaMessage.NoOp(termId = 3))?.termSeq)
    }

    @Test
    fun `the catalog reads back the boundary's term position from the block`() {
        fun catalogWith(boundaryTermSeq: Long?) =
            TableCatalog(mockk<BufferPool>(relaxed = true)).also { cat ->
                cat.refresh(
                    cat.buildBlock(
                        blockIndex = 0,
                        latestCompletedTx = null,
                        latestProcessedMsgId = 100,
                        boundaryReplicaMsgId = 42,
                        tables = emptySet(),
                        secondaryDatabases = null,
                        termId = 3,
                        boundaryTermSeq = boundaryTermSeq,
                    )
                )
            }

        assertEquals(0L, catalogWith(0).boundaryTermSeq)
        assertEquals(17L, catalogWith(17).boundaryTermSeq)
        assertNull(catalogWith(null).boundaryTermSeq)
    }
}
