package xtdb.database

import com.google.protobuf.StringValue
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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
import xtdb.api.log.*
import xtdb.catalog.BlockCatalog
import xtdb.compactor.Compactor
import xtdb.error.Incorrect
import xtdb.indexer.BlockUploader
import xtdb.indexer.ExternalSourceProcessor
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
import com.google.protobuf.Any as ProtoAny

class ExternalSourceTest {

    private lateinit var allocator: RootAllocator
    private lateinit var nodeBase: NodeBase
    private lateinit var bufferPool: MemoryStorage

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        nodeBase = openBase()
        bufferPool = MemoryStorage(allocator, 0)
    }

    @AfterEach
    fun tearDown() {
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
        liveIndex: LiveIndex = mockk(relaxed = true),
        watchers: Watchers = Watchers(-1),
        extSource: ExternalSource = InMemoryExternalSource(),
        afterToken: ExternalSourceToken? = null,
        ctx: CoroutineContext,
    ): ExternalSourceProcessor {
        val blockCatalog = BlockCatalog("test", null)
        val trieCatalog = mockk<xtdb.trie.TrieCatalog>(relaxed = true)
        val tableCatalog = mockk<xtdb.catalog.TableCatalog>(relaxed = true)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null)

        return ExternalSourceProcessor(
            allocator, nodeBase, dbStorage, dbState, blockUploader, watchers, extSource, replicaProducer,
            partition = 0, afterSourceMsgId = -1, afterReplicaMsgId = -1, afterToken = afterToken, ctx = ctx
        )
    }

    @Test
    fun `indexTx appends ResolvedTx to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
        }
        val extSource = InMemoryExternalSource()

        leaderProc(
            replicaLog = replicaLog,
            liveIndex = liveIndex,
            watchers = watchers,
            extSource = extSource,
            ctx = coroutineContext
        ).use {
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
        }
    }

    @Test
    fun `subscription routes external events through commitTx`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val liveIndex = mockk<LiveIndex>(relaxed = true)
        val extSource = InMemoryExternalSource()

        val lp = leaderProc(
            replicaLog = replicaLog, watchers = watchers, liveIndex = liveIndex,
            extSource = extSource, ctx = coroutineContext
        )

        try {
            extSource.channel.send(null)
            extSource.channel.send(null)

            delay(500.milliseconds)

            verify(exactly = 2) { liveIndex.commitTx(any()) }
            assertTrue(replicaLog.latestSubmittedOffset >= 1, "replica log should have 2 messages")
        } finally {
            lp.close()
        }
    }

    @Test
    fun `processRecords flows source records through the leader processor`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
        }
        val lp = leaderProc(replicaLog = replicaLog, liveIndex = liveIndex, ctx = coroutineContext)

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
        val watchers = Watchers(-1)
        val extSource = InMemoryExternalSource()
        val lp = leaderProc(watchers = watchers, extSource = extSource, ctx = coroutineContext)

        try {
            val token = ProtoAny.pack(StringValue.of("kafka-offset:42"))
            extSource.channel.send(token)

            delay(500.milliseconds)

            val watcherToken = watchers.externalSourceToken
            assertNotNull(watcherToken)
            assertEquals("kafka-offset:42", watcherToken!!.unpack(StringValue::class.java).value)
        } finally {
            lp.close()
        }
    }

    @Test
    fun `error in external source propagates to watchers`() = runTest {
        val watchers = Watchers(-1)

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

        leaderProc(watchers = watchers, extSource = failingSource, ctx = coroutineContext).use {
            delay(500.milliseconds)
            assertNotNull(watchers.exception, "watchers should be in failed state")
        }
    }

    @Test
    fun `error in commitTx propagates to watchers`() = runTest {
        val watchers = Watchers(-1)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { commitTx(any()) } throws RuntimeException("commit failed")
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
        val db = Database(
            allocator = allocator,
            config = config,
            storage = DatabaseStorage(null, null, null, null),
            queryState = DatabaseState("cdc", null, null, null, null),
            isIndexing = false,
            watchers = Watchers(-1),
            meterRegistry = null,
        )

        val ex = assertThrows(Incorrect::class.java) {
            db.submitTxBlocking(emptyList(), TxOpts(defaultTz = ZoneId.of("UTC")))
        }
        assertTrue(ex.message!!.contains("external source"), "message mentions external source")
    }
}
