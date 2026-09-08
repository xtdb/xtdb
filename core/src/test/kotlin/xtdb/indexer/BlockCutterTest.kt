package xtdb.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.tx.TxIndexer
import xtdb.log.proto.trieMetadata
import xtdb.table.fromSchemaAndTable
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds

/**
 * The leader's block cycle: what the boundary it cuts carries, and what reaches the replica log by the
 * time the block is closed.
 *
 * Driven through a running term, because the cycle spans the resolve side and the consume-back — the
 * boundary is appended by one and the upload is triggered by the other reading it.
 */
internal class BlockCutterTest : LeaderTermTest() {

    @Test
    fun `block finishing writes BlockBoundary + BlockUploaded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val finishedBlock = LiveTable.FinishedBlock(
            vecTypes = emptyMap(),
            rowCount = 10,
            hllDeltas = emptyMap(),
            writtenTrie = LiveTable.FinishedBlock.WrittenTrie(
                trieKey = "test-trie",
                dataFileSize = 42,
                trieMetadata = trieMetadata {}
            )
        )
        val tableRef = fromSchemaAndTable("public/foo")

        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog,
            liveIndex = liveIndexMock {
                coEvery { finishBlock(any(), any()) } returns mapOf(tableRef to finishedBlock)
                every { latestCompletedTx } returns null
            },
            watchers = watchers,
            extSource = null,
        )

        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 0, Instant.now(), SourceMessage.FlushBlock(-1))
        ))
        watchers.awaitSource(0)

        val replicaMessages = mutableListOf<ReplicaMessage>()
        backgroundScope.launch {
            replicaLog.tailAll(0, -1) { records -> replicaMessages.addAll(records.map { it.message }) }
        }

        delay(200)

        assertEquals(2, replicaMessages.size, "expected 2 replica messages, got: $replicaMessages")
        assertTrue(replicaMessages[0] is ReplicaMessage.BlockBoundary)
        assertTrue(replicaMessages[1] is ReplicaMessage.BlockUploaded)

        val boundary = replicaMessages[0] as ReplicaMessage.BlockBoundary
        assertEquals(0, boundary.blockIndex)

        val uploaded = replicaMessages[1] as ReplicaMessage.BlockUploaded
        assertEquals(0, uploaded.blockIndex)
        assertTrue(uploaded.tries.isNotEmpty(), "BlockUploaded should contain trie details")
    }

    @Test
    fun `block boundaries carry the latest external-source token, not the last tx's`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog,
            liveIndex = liveIndexMock {
                coEvery { finishBlock(any(), any()) } returns emptyMap()
                every { latestCompletedTx } returns null
            },
            watchers = watchers,
            extSource = mockk(relaxed = true),
            skipTxs = setOf(10),
        )

        val token = byteArrayOf(1, 2, 3)

        // The ext-source tx carries the CDC resume token; awaiting its durability (txId 0) pins the
        // ordering — it resolves and applies before the token-less source-log tx that follows.
        lp.extSrcProc!!.submitTx(token) { TxIndexer.TxResult.Committed() }
        watchers.awaitTx(0)

        // A token-less source-log tx (msgId 10; skipTxs covers it, so no Arrow payload needed, and its
        // txId must exceed the ext tx's for watchers' monotonicity). It resolves behind the ext tx.
        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 10, Instant.now(), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
        ))

        // Force the cut with a FlushBlock: the block's last tx is the token-less source-log tx, so the
        // boundary must carry the earlier ext tx's token (the last non-null token seen), not a null one.
        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 11, Instant.now(), SourceMessage.FlushBlock(-1))
        ))
        watchers.awaitSource(11)

        val boundaries = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.BlockBoundary }.toList()

        assertEquals(1, boundaries.size, "exactly one BlockBoundary should be written")
        assertArrayEquals(
            token, boundaries.single().externalSourceToken,
            "BlockBoundary must carry the ext-source tx's token, not the source-log tx's null token"
        )
    }
}
