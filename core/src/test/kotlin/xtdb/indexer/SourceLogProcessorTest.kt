package xtdb.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.DatabaseName
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.api.tx.TxIndexer
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.table.fromSchemaAndTable
import xtdb.trie.Trie
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds

/**
 * The resolve side of a leader term: what a source-log record resolves to, and what reaches the replica
 * log as a result.
 *
 * Driven through a running term rather than a bare [SourceLogProcessor], because what a record resolves to
 * is only observable once the append pump has written it — and a dbOp's verdict depends on what the
 * resolver has already queued, which needs a term to queue against.
 */
internal class SourceLogProcessorTest : LeaderTermTest() {

    @Test
    fun `a second attach resolved before the first is read back is refused`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        // Refuses nothing, so a refusal here can only have come from the resolver's own queued dbOps.
        val dbCatalog = RecordingDbCatalog()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog, watchers = watchers, dbCatalog = dbCatalog,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        backgroundScope.launch {
            lp.srcLogProc.processRecords(
                (0L..1L).map {
                    Log.Record(0, it, Instant.now(), SourceMessage.AttachDatabase("new_db", Database.Config()))
                }
            )
        }

        appendStarted.await()
        testScheduler.advanceUntilIdle()

        gate.complete(Unit)
        watchers.awaitTx(1)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()

        assertEquals(listOf(true, false), resolvedTxs.map { it.committed })
        assertEquals(listOf("new_db"), dbCatalog.attached, "only the first attach reaches the catalog")
    }

    @Test
    fun `a detach of a name attached but not yet read back is allowed`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        val dbCatalog = RecordingDbCatalog()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog, watchers = watchers, dbCatalog = dbCatalog,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        backgroundScope.launch {
            lp.srcLogProc.processRecords(
                listOf(
                    Log.Record(0, 0, Instant.now(), SourceMessage.AttachDatabase("new_db", Database.Config())),
                    Log.Record(0, 1, Instant.now(), SourceMessage.DetachDatabase("new_db")),
                )
            )
        }

        appendStarted.await()
        gate.complete(Unit)
        watchers.awaitTx(1)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()

        assertEquals(listOf(true, true), resolvedTxs.map { it.committed })
        assertEquals(emptyList<DatabaseName>(), dbCatalog.attached, "attached, then detached")
    }

    @Test
    fun `a second detach resolved before the first is read back is refused`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        val dbCatalog = RecordingDbCatalog()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog, watchers = watchers, dbCatalog = dbCatalog,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        backgroundScope.launch {
            lp.srcLogProc.processRecords(
                (0L..1L).map {
                    Log.Record(0, it, Instant.now(), SourceMessage.DetachDatabase("new_db"))
                }
            )
        }

        appendStarted.await()
        gate.complete(Unit)
        watchers.awaitTx(1)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()

        assertEquals(listOf(true, false), resolvedTxs.map { it.committed })
    }

    @Test
    fun `TriesAdded forwarded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val trieCatalog = createTrieCatalog()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler),
            replicaLog = replicaLog, trieCatalog = trieCatalog, watchers = watchers
        )

        // the catalog silently drops a trie whose key it can't parse, so this has to be a real one
        val trieKey = Trie.l0Key(0).toString()

        val tries = listOf(
            TrieDetails.newBuilder()
                .setTableName("public/foo")
                .setTrieKey(trieKey)
                .setDataFileSize(100)
                .setTrieMetadata(trieMetadata {})
                .build()
        )

        lp.srcLogProc.processRecords(
            listOf(
                Log.Record(0, 0, Instant.now(), SourceMessage.TriesAdded(Storage.VERSION, 0, tries))
            )
        )
        watchers.awaitSource(0)

        assertEquals(
            listOf(trieKey), trieCatalog.listAllTrieKeys(fromSchemaAndTable("public/foo")),
            "the trie is in the catalog"
        )
        assertTrue(replicaLog.latestSubmittedOffset() >= 0, "replica log should have received a message")
    }

    @Test
    fun `FlushBlock ignored when CAS does not match`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers)

        lp.srcLogProc.processRecords(
            listOf(
                Log.Record(0, 0, Instant.now(), SourceMessage.FlushBlock(5))
            )
        )
        watchers.awaitSource(0)

        val boundaries = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.BlockBoundary }.toList()

        assertEquals(emptyList<ReplicaMessage.BlockBoundary>(), boundaries)
    }

    @Test
    fun `a FlushBlock cut carries the latest external-source token, not the last tx's`() =
        runTest(timeout = 5.seconds) {
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
            lp.srcLogProc.processRecords(
                listOf(
                    Log.Record(0, 10, Instant.now(), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
                )
            )

            lp.srcLogProc.processRecords(
                listOf(
                    Log.Record(0, 11, Instant.now(), SourceMessage.FlushBlock(-1))
                )
            )
            watchers.awaitSource(11)

            val boundaries = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
                .mapNotNull { it.message as? ReplicaMessage.BlockBoundary }.toList()

            assertArrayEquals(
                token, boundaries.single().externalSourceToken,
                "the boundary carries the last non-null token seen, not the token of the tx it follows"
            )
        }
}
