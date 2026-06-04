package xtdb.database

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.log.*
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.error.Incorrect
import xtdb.indexer.BlockUploader
import xtdb.indexer.CrashLogger
import xtdb.indexer.LeaderLogProcessor
import xtdb.indexer.LiveIndex
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import xtdb.storage.MemoryStorage
import xtdb.tx.TxOpts
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.milliseconds

class ExternalSourceTest {

    private lateinit var allocator: RootAllocator
    private lateinit var nodeBase: NodeBase
    private lateinit var bufferPool: MemoryStorage
    private lateinit var liveIndex: LiveIndex

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        nodeBase = openBase()
        bufferPool = MemoryStorage(allocator, 0)

        val blockCatalog = BlockCatalog("test", null)
        val tableCatalog = TableCatalog(bufferPool).also { it.refresh(blockCatalog) }
        val trieCatalog = createTrieCatalog()
        liveIndex = LiveIndex.open(allocator, blockCatalog, tableCatalog, trieCatalog, "test")
    }

    @AfterEach
    fun tearDown() {
        liveIndex.close()
        bufferPool.close()
        nodeBase.close()
        allocator.close()
    }

    /**
     * Simple in-memory ExternalSource for testing.
     * Send signals to [channel]; each signal submits a tx via [xtdb.indexer.TxIndexer.indexTx].
     */
    class InMemoryExternalSource(
        val channel: Channel<ExternalSourceToken?> = Channel(100),
    ) : ExternalSource {

        override suspend fun onPartitionAssigned(
            partition: Int,
            afterToken: ExternalSourceToken?,
            txIndexer: TxIndexer
        ) {
            for (token in channel) {
                txIndexer.indexTx(token) { _ ->
                    TxResult.Committed()
                }
            }
        }

        override fun close() {
            channel.close()
        }
    }

    private fun leaderProc(
        sourceLog: InMemoryLog<SourceMessage> = InMemoryLog(InstantSource.system(), 0),
        replicaLog: InMemoryLog<ReplicaMessage> = InMemoryLog(InstantSource.system(), 0),
        liveIndex: LiveIndex = this.liveIndex,
        watchers: Watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1),
        extSource: ExternalSource = InMemoryExternalSource(),
        afterToken: ExternalSourceToken? = null,
        ctx: CoroutineContext,
    ): LeaderLogProcessor {
        val blockCatalog = BlockCatalog("test", null)
        val trieCatalog = mockk<xtdb.trie.TrieCatalog>(relaxed = true)
        val tableCatalog = mockk<xtdb.catalog.TableCatalog>(relaxed = true)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null, null)

        val crashLogger = mockk<CrashLogger>(relaxed = true)

        return LeaderLogProcessor(
            allocator, nodeBase, dbStorage, crashLogger,
            dbState, blockUploader, watchers, extSource, replicaProducer,
            skipTxs = emptySet(), dbCatalog = null,
            partition = 0, afterReplicaMsgId = -1, ctx = ctx
        )
    }

    @Test
    fun `indexTx appends ResolvedTx to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val extSource = InMemoryExternalSource()

        val lp = leaderProc(replicaLog = replicaLog, extSource = extSource, ctx = coroutineContext)
        try {
            extSource.channel.send(null)
            delay(500.milliseconds)

            assertTrue(replicaLog.latestSubmittedOffset >= 0, "replica log should have received a message")

            val replicaMessages = mutableListOf<ReplicaMessage>()
            val job = launch {
                replicaLog.tailAll(-1) { records ->
                    replicaMessages.addAll(records.map { it.message })
                }
            }
            delay(200.milliseconds)
            job.cancelAndJoin()

            assertEquals(1, replicaMessages.size)
            val resolved = replicaMessages[0] as ReplicaMessage.ResolvedTx
            assertEquals(true, resolved.committed)
            assertEquals(0L, resolved.txId)
            assertNull(resolved.srcMsgId, "ext-source ResolvedTx should not carry a source-log msgId")
        } finally {
            lp.close()
        }
    }

    @Test
    fun `successive external events appear in the replica log with monotonic txIds`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val extSource = InMemoryExternalSource()

        val lp = leaderProc(replicaLog = replicaLog, extSource = extSource, ctx = coroutineContext)

        try {
            extSource.channel.send(null)
            extSource.channel.send(null)
            delay(500.milliseconds)

            val resolvedTxs = mutableListOf<ReplicaMessage.ResolvedTx>()
            val job = launch {
                replicaLog.tailAll(-1) { records ->
                    records.forEach { (it.message as? ReplicaMessage.ResolvedTx)?.let(resolvedTxs::add) }
                }
            }
            delay(200.milliseconds)
            job.cancelAndJoin()

            assertEquals(listOf(0L, 1L), resolvedTxs.map { it.txId })
            assertTrue(resolvedTxs.all { it.committed }, "both txs should be committed")
        } finally {
            lp.close()
        }
    }

    @Test
    fun `processRecords flows source records through the leader processor`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val lp = leaderProc(replicaLog = replicaLog, ctx = coroutineContext)

        try {
            val now = Instant.now()

            lp.processRecords(
                listOf(
                    Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
                )
            )

            delay(500.milliseconds)

            assertTrue(replicaLog.latestSubmittedOffset >= 0, "source records should flow through to the leader")
        } finally {
            lp.close()
        }
    }

    @Test
    fun `indexTx threads resumeToken to watchers`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val extSource = InMemoryExternalSource()
        val lp = leaderProc(watchers = watchers, extSource = extSource, ctx = coroutineContext)

        try {
            val token = "kafka-offset:42".toByteArray()
            extSource.channel.send(token)

            delay(500.milliseconds)

            val watcherToken = watchers.externalSourceToken
            assertNotNull(watcherToken)
            assertArrayEquals(token, watcherToken)
        } finally {
            lp.close()
        }
    }

    @Test
    fun `error in external source propagates to watchers`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val failingSource = object : ExternalSource {
            override suspend fun onPartitionAssigned(
                partition: Int,
                afterToken: ExternalSourceToken?,
                txIndexer: TxIndexer
            ) {
                throw RuntimeException("source poll failed")
            }

            override fun close() {}
        }

        val lp = leaderProc(watchers = watchers, extSource = failingSource, ctx = coroutineContext)
        try {
            delay(500.milliseconds)
            assertNotNull(watchers.exception, "watchers should be in failed state")
        } finally {
            lp.close()
        }
    }

    @Test
    fun `fault in the commit pipeline tips watchers into Failed`() = runTest {
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { importTx(any()) } throws RuntimeException("commit pipeline fault")
        }

        val extSource = InMemoryExternalSource()
        val lp = leaderProc(watchers = watchers, liveIndex = liveIndex, extSource = extSource, ctx = coroutineContext)

        try {
            extSource.channel.send(null)
            delay(500.milliseconds)
            assertNotNull(watchers.exception, "watchers should be in failed state")
        } finally {
            lp.close()
        }
    }

    @Test
    fun `submitTxBlocking rejects when externalSource is configured`() {
        val extFactory = mockk<ExternalSource.Factory>()
        val config = Database.Config(externalSource = extFactory)

        // note: not .use — Database.close() would close `allocator`, which @AfterEach also closes
        val partition = DatabasePartition(
            partition = 0,
            state = DatabaseState("cdc", null, null, null, null),
            watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1),
        )
        val db = Database(
            allocator = allocator,
            config = config,
            storage = DatabaseStorage(null, null, null, null),
            isIndexing = false,
            partitions = mapOf(0 to partition),
            meterRegistry = null,
        )

        val ex = assertThrows(Incorrect::class.java) {
            db.submitTxBlocking(emptyList(), TxOpts(defaultTz = ZoneId.of("UTC")))
        }
        assertTrue(ex.message!!.contains("external source"), "message mentions external source")
    }
}
