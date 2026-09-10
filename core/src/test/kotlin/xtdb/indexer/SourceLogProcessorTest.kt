package xtdb.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.api.tx.TxIndexer
import xtdb.catalog.TableCatalog
import xtdb.database.Database
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.storage.BufferPool
import xtdb.table.fromSchemaAndTable
import xtdb.trie.Trie
import xtdb.trie.TrieCatalog
import xtdb.types.MessageId
import xtdb.util.closeAll
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.seconds

/**
 * The resolve side of a leader term: what a source-log record resolves to, and what reaches the replica
 * log as a result.
 *
 * Driven through [SourceLogProcessor.handleRecord] rather than `processRecords`, because the batch pipe is
 * the transport's edge and nothing here is the transport. A dbOp's verdict turns on what the resolver has
 * staged and not yet read back, which is what resolving without applying leaves it holding.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal class SourceLogProcessorTest : LeaderTermTest() {

    private val resolversToClose = mutableListOf<AutoCloseable>()

    @AfterEach
    fun closeResolvers() = resolversToClose.closeAll()

    private class ResolveSide(
        val srcLogProc: SourceLogProcessor,
        private val driver: RecordingLogsDriver,
    ) {
        val appended get() = driver.appended.toList()

        /** Whether each resolved tx committed, in resolution order. */
        val verdicts get() = appended.filterIsInstance<ReplicaMessage.ResolvedTx>().map { it.committed }
    }

    /**
     * A resolve side with no term: the processor, the cutter it stages rows onto, and the append pump
     * drained into a recording driver. The pump writes from its own coroutine, so what a record resolved
     * to lands on the next `runCurrent`.
     */
    private fun TestScope.resolveSide(
        // Supplying one names this database 'xtdb', since only the primary's leader holds a catalog.
        dbCatalog: Database.Catalog? = null,
        trieCatalog: TrieCatalog = createTrieCatalog(),
        skipTxs: Set<MessageId> = emptySet(),
    ): ResolveSide {
        val dbName = if (dbCatalog != null) "xtdb" else "test"
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val partitionState = PartitionState(TableCatalog(bufferPool), trieCatalog, liveIndexMock())
        val partitionStorage = PartitionStorage(
            DatabaseLogs(
                InMemoryLog<SourceMessage>(InstantSource.system(), 0),
                InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
            ),
            bufferPool, null
        )

        val driver = RecordingLogsDriver()
        val appender = ReplicaLogAppender(driver)
        backgroundScope.launch { appender.run() }

        val txResolver = TxResolver(
            allocator, nodeBase, partitionStorage, partitionState, dbName, mockk(relaxed = true), skipTxs,
            resolvedSrcMsgId = -1, resolvedExtToken = null, InstantSource.system()
        ).also { resolversToClose += it }

        val blockCutter = BlockCutter(
            partitionStorage, partitionState, dbName, leaderTerm = 1, replicaAppender = appender,
            logsDriver = driver, compactor = mockk(relaxed = true), dbCatalog = null, meterRegistry = null,
            lastUploadEpochSeconds = AtomicLong(0), scope = backgroundScope,
            ioDispatcher = StandardTestDispatcher(testScheduler)
        )

        return ResolveSide(
            SourceLogProcessor(
                partitionStorage, partitionState, dbCatalog, dbName, 1,
                driver, txResolver, blockCutter, appender, IndexerConfig().flushDuration
            ),
            driver
        )
    }

    private fun record(msgId: MessageId, msg: SourceMessage) = Log.Record(0, msgId, Instant.now(), msg)

    @Test
    fun `a second attach resolved before the first is read back is refused`() = runTest {
        // Refuses nothing, so a refusal here can only have come from the resolver's own staged dbOps.
        val rs = resolveSide(dbCatalog = RecordingDbCatalog())

        rs.srcLogProc.handleRecord(record(0, SourceMessage.AttachDatabase("new_db", Database.Config())))
        rs.srcLogProc.handleRecord(record(1, SourceMessage.AttachDatabase("new_db", Database.Config())))
        runCurrent()

        assertEquals(listOf(true, false), rs.verdicts)
    }

    @Test
    fun `a detach of a name attached but not yet read back is allowed`() = runTest {
        val rs = resolveSide(dbCatalog = RecordingDbCatalog())

        rs.srcLogProc.handleRecord(record(0, SourceMessage.AttachDatabase("new_db", Database.Config())))
        rs.srcLogProc.handleRecord(record(1, SourceMessage.DetachDatabase("new_db")))
        runCurrent()

        assertEquals(listOf(true, true), rs.verdicts)
    }

    @Test
    fun `a second detach resolved before the first is read back is refused`() = runTest {
        val rs = resolveSide(dbCatalog = RecordingDbCatalog())

        rs.srcLogProc.handleRecord(record(0, SourceMessage.DetachDatabase("new_db")))
        rs.srcLogProc.handleRecord(record(1, SourceMessage.DetachDatabase("new_db")))
        runCurrent()

        assertEquals(listOf(true, false), rs.verdicts)
    }

    @Test
    fun `TriesAdded reaches the local catalog and the replica log`() = runTest {
        val trieCatalog = createTrieCatalog()
        val rs = resolveSide(trieCatalog = trieCatalog)

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

        rs.srcLogProc.handleRecord(record(0, SourceMessage.TriesAdded(Storage.VERSION, 0, tries)))
        runCurrent()

        assertEquals(
            listOf(trieKey), trieCatalog.listAllTrieKeys(fromSchemaAndTable("public/foo")),
            "the resolve side updates the catalog itself, ahead of its own consume-back"
        )
        assertEquals(
            listOf(trieKey),
            rs.appended.filterIsInstance<ReplicaMessage.TriesAdded>().flatMap { it.tries }.map { it.trieKey },
            "and replicates it for the followers"
        )
    }

    @Test
    fun `FlushBlock ignored when CAS does not match`() = runTest {
        val rs = resolveSide()

        rs.srcLogProc.handleRecord(record(0, SourceMessage.FlushBlock(5)))
        runCurrent()

        assertEquals(
            emptyList<ReplicaMessage.BlockBoundary>(),
            rs.appended.filterIsInstance<ReplicaMessage.BlockBoundary>()
        )
        assertEquals(
            listOf(0L), rs.appended.filterIsInstance<ReplicaMessage.NoOp>().map { it.srcMsgId },
            "the position still advances, so the watermark does not stall on a refused flush (#5680)"
        )
    }

    @Test
    fun `FlushBlock cuts a block when its CAS matches`() = runTest {
        val rs = resolveSide()

        // -1 is the CAS for 'no block cut yet', which is where a fresh database starts.
        rs.srcLogProc.handleRecord(record(0, SourceMessage.FlushBlock(-1)))
        runCurrent()

        assertEquals(
            listOf(0L),
            rs.appended.filterIsInstance<ReplicaMessage.BlockBoundary>().map { it.blockIndex }
        )
        assertEquals(
            emptyList<Long>(), rs.appended.filterIsInstance<ReplicaMessage.NoOp>().map { it.srcMsgId },
            "the boundary carries the position, so there is nothing for a NoOp to advance"
        )
    }

    @Test
    fun `a FlushBlock cut carries the latest external-source token, not the last tx's`() =
        runTest(timeout = 5.seconds) {
            val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
            val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

            // A term, not a bare resolve side: what this turns on is the ext tx applying before the
            // source-log tx resolves, and applying is the consume-back's.
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

            lp.srcLogProc.processRecords(listOf(
                Log.Record(0, 11, Instant.now(), SourceMessage.FlushBlock(-1))
            ))
            watchers.awaitSource(11)

            val boundaries = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
                .mapNotNull { it.message as? ReplicaMessage.BlockBoundary }.toList()

            assertArrayEquals(
                token, boundaries.single().externalSourceToken,
                "the boundary carries the last non-null token seen, not the token of the tx it follows"
            )
        }
}
