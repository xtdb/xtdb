package xtdb.database

import com.google.protobuf.StringValue
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.TransactionResult
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.compactor.Compactor
import xtdb.error.Incorrect
import xtdb.indexer.*
import xtdb.storage.MemoryStorage
import xtdb.tx.TxOpts
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import com.google.protobuf.Any as ProtoAny

class ExternalSourceTest {

    private lateinit var allocator: RootAllocator
    private lateinit var bufferPool: MemoryStorage

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        bufferPool = MemoryStorage(allocator, 0)
    }

    @AfterEach
    fun tearDown() {
        bufferPool.close()
        allocator.close()
    }

    private fun leaderProc(
        sourceLog: InMemoryLog<SourceMessage> = InMemoryLog(InstantSource.system(), 0),
        replicaLog: InMemoryLog<ReplicaMessage> = InMemoryLog(InstantSource.system(), 0),
        liveIndex: LiveIndex = mockk(relaxed = true),
        watchers: Watchers = Watchers(-1),
    ): LeaderLogProcessor {
        val blockCatalog = BlockCatalog("test", null)
        val trieCatalog = mockk<xtdb.trie.TrieCatalog>(relaxed = true)
        val tableCatalog = mockk<xtdb.catalog.TableCatalog>(relaxed = true)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null)

        return LeaderLogProcessor(
            allocator, dbStorage, replicaProducer,
            dbState, mockk(relaxed = true), watchers,
            emptySet(), null, blockUploader, afterSourceMsgId = -1, afterReplicaMsgId = -1
        )
    }

    /**
     * Simple in-memory ExternalSource for testing.
     * Send signals to [channel] to trigger txHandler.handleTx with a mock OpenTx.
     */
    class InMemoryExternalSource(
        val channel: Channel<Unit> = Channel(100)
    ) : ExternalSource {

        override suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txHandler: ExternalSource.TxHandler) {
            for (signal in channel) {
                txHandler.handleTx(mockk(relaxed = true), null)
            }
        }

        override fun close() {
            channel.close()
        }
    }

    @Test
    fun `handleExternalTx appends to replica log and notifies watchers`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val now = Instant.now()
        lp.handleExternalTx(
            ReplicaMessage.ResolvedTx(
                txId = 0, systemTime = now,
                committed = true, error = null,
                tableData = emptyMap(),
            )
        )

        assertTrue(replicaLog.latestSubmittedOffset >= 0, "replica log should have received a message")

        val replicaMessages = mutableListOf<ReplicaMessage>()
        val job = launch { replicaLog.tailAll(-1) { records ->
            replicaMessages.addAll(records.map { it.message })
        } }
        delay(200)
        job.cancelAndJoin()

        assertEquals(1, replicaMessages.size)
        val resolved = replicaMessages[0] as ReplicaMessage.ResolvedTx
        assertEquals(true, resolved.committed)
        assertEquals(0, resolved.txId)
    }

    @Test
    fun `Demux routes external events to txHandler`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val extSource = InMemoryExternalSource()
        val events = mutableListOf<String>()

        val txHandler = ExternalSource.TxHandler { _, _ ->
            events.add("handled")
            lp.handleExternalTx(
                ReplicaMessage.ResolvedTx(
                    txId = 0, systemTime = Instant.now(),
                    committed = true, error = null,
                    tableData = emptyMap(),
                )
            )
        }

        val demux = ExternalSource.Demux(lp, extSource, 0, null, txHandler, coroutineContext)

        try {
            extSource.channel.send(Unit)
            extSource.channel.send(Unit)

            delay(500)

            assertEquals(2, events.size)
            assertTrue(replicaLog.latestSubmittedOffset >= 1, "replica log should have 2 messages")
        } finally {
            demux.close()
        }
    }

    @Test
    fun `Demux routes source records to leader processor`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
        }
        val lp = leaderProc(replicaLog = replicaLog, liveIndex = liveIndex)

        val extSource = InMemoryExternalSource()
        val txHandler = ExternalSource.TxHandler { _, _ -> }

        val demux = ExternalSource.Demux(lp, extSource, 0, null, txHandler, coroutineContext)

        try {
            val now = Instant.now()

            demux.processRecords(listOf(
                Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
            ))

            delay(500)

            assertTrue(replicaLog.latestSubmittedOffset >= 0, "source records should flow through demux to leader")
        } finally {
            demux.close()
        }
    }

    @Test
    fun `handleExternalTx with aborted transaction`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val now = Instant.now()
        lp.handleExternalTx(
            ReplicaMessage.ResolvedTx(
                txId = 0, systemTime = now,
                committed = false, error = RuntimeException("bad data"),
                tableData = emptyMap(),
            )
        )

        val result = watchers.awaitTx(0)
        assertNotNull(result)
        assertInstanceOf(TransactionResult.Aborted::class.java, result)

        val replicaMessages = mutableListOf<ReplicaMessage>()
        val job = launch { replicaLog.tailAll(-1) { records ->
            replicaMessages.addAll(records.map { it.message })
        } }
        delay(200)
        job.cancelAndJoin()

        assertEquals(1, replicaMessages.size)
        val resolved = replicaMessages[0] as ReplicaMessage.ResolvedTx
        assertEquals(false, resolved.committed)
    }

    @Test
    fun `handleExternalTx threads externalSourceToken to watchers`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val token = ProtoAny.pack(StringValue.of("kafka-offset:42"))
        val now = Instant.now()

        lp.handleExternalTx(
            ReplicaMessage.ResolvedTx(
                txId = 0, systemTime = now,
                committed = true, error = null,
                tableData = emptyMap(),
                externalSourceToken = token,
            )
        )

        val watcherToken = watchers.externalSourceToken
        assertNotNull(watcherToken)
        assertEquals("kafka-offset:42", watcherToken!!.unpack(StringValue::class.java).value)
    }

    @Test
    fun `Demux error in external source propagates to watchers`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val failingSource = object : ExternalSource {
            override suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txHandler: ExternalSource.TxHandler) {
                throw RuntimeException("source poll failed")
            }
            override fun close() {}
        }

        val txHandler = ExternalSource.TxHandler { _, _ -> }

        val demux = ExternalSource.Demux(lp, failingSource, 0, null, txHandler, coroutineContext)

        try {
            delay(500)

            assertNotNull(watchers.exception, "watchers should be in failed state")
        } finally {
            demux.close()
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

    @Test
    fun `Demux error in txHandler propagates to watchers`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val extSource = InMemoryExternalSource()

        val txHandler = ExternalSource.TxHandler { _, _ ->
            throw RuntimeException("handler failed")
        }

        val demux = ExternalSource.Demux(lp, extSource, 0, null, txHandler, coroutineContext)

        try {
            extSource.channel.send(Unit)

            delay(500)

            assertNotNull(watchers.exception, "watchers should be in failed state")
        } finally {
            demux.close()
        }
    }
}
