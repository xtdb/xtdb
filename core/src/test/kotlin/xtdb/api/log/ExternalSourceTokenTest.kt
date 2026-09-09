package xtdb.api.log

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.block.proto.Block
import xtdb.catalog.TableCatalog
import xtdb.storage.BufferPool

class ExternalSourceTokenTest {

    private val testToken: ByteArray = "kafka-offset:42".toByteArray()
    private val term = LeaderTerm.of(0, 1)

    @Test
    fun `BlockBoundary round-trips external source token`() {
        val boundary = ReplicaMessage.BlockBoundary(1, 100, testToken, termId = term)
        val encoded = boundary.encode()
        val decoded = ReplicaMessage.decode(encoded)

        assertInstanceOf(ReplicaMessage.BlockBoundary::class.java, decoded)
        decoded as ReplicaMessage.BlockBoundary
        assertEquals(1, decoded.blockIndex)
        assertEquals(100, decoded.latestProcessedMsgId)
        assertNotNull(decoded.externalSourceToken)
        assertArrayEquals(testToken, decoded.externalSourceToken)
    }

    @Test
    fun `BlockBoundary round-trips without token`() {
        val boundary = ReplicaMessage.BlockBoundary(1, 100, termId = term)
        val encoded = boundary.encode()
        val decoded = ReplicaMessage.decode(encoded) as ReplicaMessage.BlockBoundary

        assertEquals(1, decoded.blockIndex)
        assertEquals(100, decoded.latestProcessedMsgId)
        assertNull(decoded.externalSourceToken)
    }

    @Test
    fun `ReplicaMessage BlockUploaded round-trips external source token`() {
        val uploaded = ReplicaMessage.BlockUploaded(1, 0, 1, 100, emptyList(), testToken, termId = term)
        val encoded = uploaded.encode()
        val decoded = ReplicaMessage.decode(encoded)

        assertInstanceOf(ReplicaMessage.BlockUploaded::class.java, decoded)
        decoded as ReplicaMessage.BlockUploaded
        assertArrayEquals(testToken, decoded.externalSourceToken)
    }

    @Test
    fun `Block proto round-trips external source token`() {
        val tableCatalog = TableCatalog(mockk<BufferPool>(relaxed = true))

        val block = tableCatalog.buildBlock(
            blockIndex = 0,
            latestCompletedTx = null,
            latestProcessedMsgId = 100,
            boundaryReplicaMsgId = null,
            tables = emptySet(),
            secondaryDatabases = null,
            externalSourceToken = testToken,
            termId = term
        )

        val parsed = Block.parseFrom(block.toByteArray())
        assertTrue(parsed.hasExternalSourceToken())
        assertArrayEquals(testToken, parsed.externalSourceToken.toByteArray())
    }

    @Test
    fun `TableCatalog externalSourceToken reads from latest block`() {
        val tableCatalog = TableCatalog(mockk<BufferPool>(relaxed = true))

        assertNull(tableCatalog.externalSourceToken)

        val block = tableCatalog.buildBlock(
            blockIndex = 0,
            latestCompletedTx = null,
            latestProcessedMsgId = 100,
            boundaryReplicaMsgId = null,
            tables = emptySet(),
            secondaryDatabases = null,
            externalSourceToken = testToken,
            termId = term
        )
        tableCatalog.refresh(block)

        assertArrayEquals(testToken, tableCatalog.externalSourceToken)
    }

    @Test
    fun `TableCatalog externalSourceToken returns null when no token`() {
        val tableCatalog = TableCatalog(mockk<BufferPool>(relaxed = true))

        val block = tableCatalog.buildBlock(
            blockIndex = 0,
            latestCompletedTx = null,
            latestProcessedMsgId = 100,
            boundaryReplicaMsgId = null,
            tables = emptySet(),
            secondaryDatabases = null,
            termId = term
        )
        tableCatalog.refresh(block)

        assertNull(tableCatalog.externalSourceToken)
    }
}
