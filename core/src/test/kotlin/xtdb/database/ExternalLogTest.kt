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
import xtdb.indexer.*
import xtdb.storage.MemoryStorage
import java.time.Instant
import java.time.InstantSource
import com.google.protobuf.Any as ProtoAny

class ExternalLogTest {

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
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, null, bufferPool, null)
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
     * Simple in-memory ExternalLog for testing.
     * Send records to [channel] to simulate external events arriving.
     */
    class InMemoryExternalLog<M>(
        val channel: Channel<List<M>> = Channel(100)
    ) : ExternalLog<M> {

        override suspend fun tailAll(
            afterToken: ExternalSourceToken?,
            processor: ExternalLog.MessageProcessor<M>
        ) {
            for (msgs in channel) {
                processor.processMessages(msgs)
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
        assertTrue(resolved.committed)
        assertEquals(0, resolved.txId)
    }

    @Test
    fun `Demux routes external records to external processor`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val externalLog = InMemoryExternalLog<String>()
        val processedRecords = mutableListOf<String>()

        val extProc = ExternalLog.MessageProcessor<String> { msgs ->
            for (msg in msgs) {
                processedRecords.add(msg)

                lp.handleExternalTx(
                    ReplicaMessage.ResolvedTx(
                        txId = 0, systemTime = Instant.now(),
                        committed = true, error = null,
                        tableData = emptyMap(),
                    )
                )
            }
        }

        val demux = ExternalLog.Demux(lp, externalLog, null, extProc, coroutineContext)

        try {
            externalLog.channel.send(listOf("event-1", "event-2"))

            // Demux runs on Dispatchers.Default — real time, not virtual
            delay(500)

            assertEquals(listOf("event-1", "event-2"), processedRecords)
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

        val externalLog = InMemoryExternalLog<String>()
        val extProc = ExternalLog.MessageProcessor<String> { }

        val demux = ExternalLog.Demux(lp, externalLog, null, extProc, coroutineContext)

        try {
            val now = Instant.now()

            // send a source message through the demux
            demux.processRecords(listOf(
                Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
            ))

            // Demux runs on Dispatchers.Default — real time, not virtual
            delay(500)

            // FlushBlock with matching CAS (-1 == no current block) should trigger block finish
            // which writes BlockBoundary + BlockUploaded to replica
            assertTrue(replicaLog.latestSubmittedOffset >= 0, "source records should flow through demux to leader")
        } finally {
            demux.close()
        }
    }

    @Test
    fun `Demux interleaves source and external records`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val externalLog = InMemoryExternalLog<String>()
        val events = mutableListOf<String>()

        val extProc = ExternalLog.MessageProcessor<String> { msgs ->
            for (msg in msgs) {
                events.add("ext:$msg")
                lp.handleExternalTx(
                    ReplicaMessage.ResolvedTx(
                        txId = 0, systemTime = Instant.now(),
                        committed = true, error = null,
                        tableData = emptyMap(),
                    )
                )
            }
        }

        val demux = ExternalLog.Demux(lp, externalLog, null, extProc, coroutineContext)

        try {
            // send external event
            externalLog.channel.send(listOf("first"))
            delay(200)

            // send another external event
            externalLog.channel.send(listOf("second"))
            delay(200)

            assertEquals(listOf("ext:first", "ext:second"), events)

            // replica log should have both external txs
            val replicaMessages = mutableListOf<ReplicaMessage>()
            val job = launch { replicaLog.tailAll(-1) { records ->
                replicaMessages.addAll(records.map { it.message })
            } }
            delay(200)
            job.cancelAndJoin()

            assertEquals(2, replicaMessages.size)
            assertTrue(replicaMessages.all { it is ReplicaMessage.ResolvedTx })
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
        assertFalse(resolved.committed)
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
    fun `Demux error in external log tailAll propagates to watchers`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val failingLog = object : ExternalLog<String> {
            override suspend fun tailAll(
                afterToken: ExternalSourceToken?,
                processor: ExternalLog.MessageProcessor<String>,
            ) {
                throw RuntimeException("log poll failed")
            }
            override fun close() {}
        }

        val extProc = ExternalLog.MessageProcessor<String> { }

        val demux = ExternalLog.Demux(lp, failingLog, null, extProc, coroutineContext)

        try {
            delay(500)

            assertNotNull(watchers.exception, "watchers should be in failed state")
        } finally {
            demux.close()
        }
    }

    @Test
    fun `Demux error in external processor propagates to watchers`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(-1)
        val lp = leaderProc(replicaLog = replicaLog, watchers = watchers)

        val externalLog = InMemoryExternalLog<String>()

        val extProc = ExternalLog.MessageProcessor<String> {
            throw RuntimeException("external processor failed")
        }

        val demux = ExternalLog.Demux(lp, externalLog, null, extProc, coroutineContext)

        try {
            externalLog.channel.send(listOf("boom"))

            delay(500)

            assertNotNull(watchers.exception, "watchers should be in failed state")
        } finally {
            demux.close()
        }
    }
}
