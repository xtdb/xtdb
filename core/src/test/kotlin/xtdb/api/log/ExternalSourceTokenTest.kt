package xtdb.api.log

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.block.proto.Block
import xtdb.catalog.BlockCatalog

class ExternalSourceTokenTest {

    private val testToken: ByteArray = "kafka-offset:42".toByteArray()

    @Test
    fun `BlockBoundary round-trips external source token`() {
        val boundary = ReplicaMessage.BlockBoundary(1, 100, testToken)
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
        val boundary = ReplicaMessage.BlockBoundary(1, 100)
        val encoded = boundary.encode()
        val decoded = ReplicaMessage.decode(encoded) as ReplicaMessage.BlockBoundary

        assertEquals(1, decoded.blockIndex)
        assertEquals(100, decoded.latestProcessedMsgId)
        assertNull(decoded.externalSourceToken)
    }

    @Test
    fun `ReplicaMessage BlockUploaded round-trips external source token`() {
        val uploaded = ReplicaMessage.BlockUploaded(1, 0, 1, 100, emptyList(), testToken)
        val encoded = uploaded.encode()
        val decoded = ReplicaMessage.decode(encoded)

        assertInstanceOf(ReplicaMessage.BlockUploaded::class.java, decoded)
        decoded as ReplicaMessage.BlockUploaded
        assertArrayEquals(testToken, decoded.externalSourceToken)
    }

    @Test
    fun `SourceMessage BlockUploaded round-trips external source token`() {
        val uploaded = SourceMessage.BlockUploaded(1, 0, 1, 100, emptyList(), testToken)
        val encoded = uploaded.encode()
        val decoded = SourceMessage.decode(encoded)

        assertInstanceOf(SourceMessage.BlockUploaded::class.java, decoded)
        decoded as SourceMessage.BlockUploaded
        assertArrayEquals(testToken, decoded.externalSourceToken)
    }

    @Test
    fun `Block proto round-trips external source token`() {
        val blockCatalog = BlockCatalog(null)

        val block = blockCatalog.buildBlock(
            blockIndex = 0,
            latestCompletedTx = null,
            latestProcessedMsgId = 100,
            boundaryReplicaMsgId = null,
            tables = emptySet(),
            secondaryDatabases = null,
            externalSourceToken = testToken
        )

        val parsed = Block.parseFrom(block.toByteArray())
        assertTrue(parsed.hasExternalSourceToken())
        assertArrayEquals(testToken, parsed.externalSourceToken.toByteArray())
    }

    @Test
    fun `BlockCatalog externalSourceToken reads from latest block`() {
        val blockCatalog = BlockCatalog(null)

        assertNull(blockCatalog.externalSourceToken)

        val block = blockCatalog.buildBlock(
            blockIndex = 0,
            latestCompletedTx = null,
            latestProcessedMsgId = 100,
            boundaryReplicaMsgId = null,
            tables = emptySet(),
            secondaryDatabases = null,
            externalSourceToken = testToken
        )
        blockCatalog.refresh(block)

        assertArrayEquals(testToken, blockCatalog.externalSourceToken)
    }

    @Test
    fun `BlockCatalog externalSourceToken returns null when no token`() {
        val blockCatalog = BlockCatalog(null)

        val block = blockCatalog.buildBlock(
            blockIndex = 0,
            latestCompletedTx = null,
            latestProcessedMsgId = 100,
            boundaryReplicaMsgId = null,
            tables = emptySet(),
            secondaryDatabases = null
        )
        blockCatalog.refresh(block)

        assertNull(blockCatalog.externalSourceToken)
    }
}
