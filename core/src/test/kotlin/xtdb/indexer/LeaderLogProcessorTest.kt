package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.NodeBase
import xtdb.api.TransactionResult
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.types.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.block.proto.TableBlock
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.storage.BufferPool
import xtdb.table.fromSchemaAndTable
import xtdb.trie.TrieCatalog
import xtdb.util.closeAll
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds
import xtdb.api.tx.TxIndexer

class LeaderLogProcessorTest {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

    // runTest cancels and joins backgroundScope before tearDown, so the leaders are quiescent here
    // and freed before `allocator` closes.
    private val leadersToClose = mutableListOf<AutoCloseable>()

    @BeforeEach
    fun setUp() {
        nodeBase = NodeBase.openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDown() {
        leadersToClose.closeAll()
        allocator.close()
        nodeBase.close()
    }

    private fun TestScope.leaderProc(
        uploadDispatcher: CoroutineDispatcher,
        sourceLog: InMemoryLog<SourceMessage> = InMemoryLog(InstantSource.system(), 0),
        replicaLog: InMemoryLog<ReplicaMessage> = InMemoryLog(InstantSource.system(), 0),
        bufferPool: BufferPool = mockk(relaxed = true) { every { epoch } returns 0 },
        liveIndex: LiveIndex = mockk(relaxed = true),
        blockCatalog: BlockCatalog = BlockCatalog("test", null),
        trieCatalog: TrieCatalog = mockk(relaxed = true),
        compactor: Compactor.ForDatabase = mockk(relaxed = true),
        watchers: Watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1),
        skipTxs: Set<MessageId> = emptySet(),
        wrapProducer: (Log.AtomicProducer<ReplicaMessage>) -> Log.AtomicProducer<ReplicaMessage> = { it },
    ): LeaderLogProcessor {
        val tableCatalog = mockk<TableCatalog>(relaxed = true)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = wrapProducer(replicaLog.openAtomicProducer("test-leader"))
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null, null, backgroundScope, uploadDispatcher)

        return LeaderLogProcessor(
            allocator, nodeBase, dbStorage, mockk(relaxed = true),
            dbState, blockUploader, watchers,
            extSource = null, replicaProducer = replicaProducer,
            skipTxs = skipTxs, dbCatalog = null,
            partition = 0, afterReplicaMsgId = -1,
            scope = backgroundScope,
        ).also(leadersToClose::add)
    }

    @Test
    fun `TriesAdded forwarded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val trieCatalog = mockk<TrieCatalog>(relaxed = true)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), replicaLog = replicaLog, trieCatalog = trieCatalog, watchers = watchers)

        val tries = listOf(
            TrieDetails.newBuilder()
                .setTableName("public/foo")
                .setTrieKey("trie-key-1")
                .setDataFileSize(100)
                .setTrieMetadata(trieMetadata {})
                .build()
        )

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.TriesAdded(Storage.VERSION, 0, tries))
        ))
        watchers.awaitSource(0)

        verify { trieCatalog.addTries(any(), any(), any()) }
        assertTrue(replicaLog.latestSubmittedOffset >= 0, "replica log should have received a message")
    }

    @Test
    fun `FlushBlock triggers block finish when CAS matches`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { finishBlock(any(), any()) } returns emptyMap()
            every { latestCompletedTx } returns null
        }
        val trieCatalog = mockk<TrieCatalog>(relaxed = true) {
            every { getPartitions(any()) } returns emptyList()
        }
        val tableCatalog = mockk<TableCatalog>(relaxed = true) {
            every { finishBlock(any(), any(), any()) } returns emptyMap()
        }
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val blockCatalog = BlockCatalog("test", null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null, null, backgroundScope, StandardTestDispatcher(testScheduler))
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = LeaderLogProcessor(
            allocator, nodeBase, dbStorage, mockk(relaxed = true),
            dbState, blockUploader, watchers,
            extSource = null, replicaProducer = replicaProducer,
            skipTxs = emptySet(), dbCatalog = null,
            partition = 0, afterReplicaMsgId = -1,
            scope = backgroundScope,
        ).also(leadersToClose::add)

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
        ))
        watchers.awaitSource(0)

        verify { liveIndex.finishBlock(any(), eq(0)) }
        verify { liveIndex.nextBlock() }
        verify { compactor.signalBlock() }
        assertTrue(replicaLog.latestSubmittedOffset >= 0, "replica log should have block messages")
    }

    @Test
    fun `FlushBlock ignored when CAS does not match`() = runTest {
        val liveIndex = mockk<LiveIndex>(relaxed = true)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), liveIndex = liveIndex, watchers = watchers)

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(5))
        ))
        watchers.awaitSource(0)

        verify(exactly = 0) { liveIndex.finishBlock(any(), any()) }
    }

    @Test
    fun `block finishing writes BlockBoundary + BlockUploaded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val finishedBlock = LiveTable.FinishedBlock(
            vecTypes = emptyMap(),
            trieKey = "test-trie",
            dataFileSize = 42,
            rowCount = 10,
            trieMetadata = trieMetadata {},
            hllDeltas = emptyMap()
        )
        val tableRef = fromSchemaAndTable("public/foo")

        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { finishBlock(any(), any()) } returns mapOf(tableRef to finishedBlock)
            every { latestCompletedTx } returns null
        }
        val trieCatalog = mockk<TrieCatalog>(relaxed = true) {
            every { getPartitions(any()) } returns emptyList()
        }
        val tableCatalog = mockk<TableCatalog>(relaxed = true) {
            every { finishBlock(any(), any(), any()) } returns mapOf(
                tableRef to TableBlock.getDefaultInstance()
            )
        }
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val blockCatalog = BlockCatalog("test", null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null, null, backgroundScope, StandardTestDispatcher(testScheduler))
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val lp = LeaderLogProcessor(
            allocator, nodeBase, dbStorage, mockk(relaxed = true),
            dbState, blockUploader, watchers,
            extSource = null, replicaProducer = replicaProducer,
            skipTxs = emptySet(), dbCatalog = null,
            partition = 0, afterReplicaMsgId = -1,
            scope = backgroundScope,
        ).also(leadersToClose::add)

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
        ))
        watchers.awaitSource(0)

        val replicaMessages = mutableListOf<ReplicaMessage>()
        backgroundScope.launch { replicaLog.tailAll(-1) { records ->
            replicaMessages.addAll(records.map { it.message })
        } }

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
    fun `a slow append double-buffers the rest of the poll batch`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        val producerBatchSizes = mutableListOf<Int>()

        // Counts each producer transaction's size, and gates every append handle so the first batch's
        // settle stalls on the gate (a slow commit-ack) without blocking any thread.
        val wrapProducer = { inner: Log.AtomicProducer<ReplicaMessage> ->
            object : Log.AtomicProducer<ReplicaMessage> {
                override fun openTx(): Log.AtomicProducer.Tx<ReplicaMessage> {
                    val tx = inner.openTx()
                    var count = 0
                    return object : Log.AtomicProducer.Tx<ReplicaMessage> by tx {
                        override fun appendMessage(message: ReplicaMessage): CompletableDeferred<Log.MessageMetadata> {
                            count++
                            val real = tx.appendMessage(message)
                            appendStarted.complete(Unit)
                            return CompletableDeferred<Log.MessageMetadata>().also { gated ->
                                backgroundScope.launch { gate.await(); gated.complete(real.await()) }
                            }
                        }

                        override fun commit() {
                            tx.commit()
                            producerBatchSizes += count
                        }
                    }
                }

                override fun close() = inner.close()
            }
        }

        // Skipped txs each stage a real (aborted) row without needing a valid tx-ops payload.
        val n = 5L
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            skipTxs = (0 until n).toSet(), wrapProducer = wrapProducer,
        )

        val now = Instant.now()
        val records = (0 until n).map {
            Log.Record(0, it, now.plusMillis(it), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
        }

        val batchJob = launch { lp.processRecords(records) }

        appendStarted.await()
        gate.complete(Unit)
        batchJob.join()

        assertEquals(
            listOf(1, 4), producerBatchSizes,
            "record 0 kicked its append alone; 1-4 resolved behind it and rode the next producer transaction"
        )

        val resolvedTxs = replicaLog.readRecords(0, replicaLog.latestSubmittedMsgId + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()
        assertEquals((0 until n).toList(), resolvedTxs.map { it.txId }, "all $n txs land, in send order")
    }

    @Test
    fun `executeTx returns only once its tx is durable`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val wrapProducer = { inner: Log.AtomicProducer<ReplicaMessage> ->
            object : Log.AtomicProducer<ReplicaMessage> {
                override fun openTx(): Log.AtomicProducer.Tx<ReplicaMessage> {
                    val tx = inner.openTx()
                    return object : Log.AtomicProducer.Tx<ReplicaMessage> by tx {
                        override fun appendMessage(message: ReplicaMessage): CompletableDeferred<Log.MessageMetadata> {
                            val real = tx.appendMessage(message)
                            appendStarted.complete(Unit)
                            return CompletableDeferred<Log.MessageMetadata>().also { gated ->
                                backgroundScope.launch { gate.await(); gated.complete(real.await()) }
                            }
                        }
                    }
                }

                override fun close() = inner.close()
            }
        }

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapProducer = wrapProducer,
        )

        // launch the executeTx so we can observe its completion state without blocking the test
        val txJob = backgroundScope.async { lp.executeTx(null) { TxIndexer.TxResult.Committed() } }

        appendStarted.await()

        assertFalse(txJob.isCompleted, "executeTx must not return before the replica-log append settles")

        gate.complete(Unit)
        val result = txJob.await()

        assertTrue(result is TransactionResult.Committed, "executeTx returns Committed once durable")
    }

    @Test
    fun `closing the leader term fails an awaiting executeTx rather than hanging`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        // Gate that is never opened — the append will stall indefinitely unless the term is cancelled.
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val wrapProducer = { inner: Log.AtomicProducer<ReplicaMessage> ->
            object : Log.AtomicProducer<ReplicaMessage> {
                override fun openTx(): Log.AtomicProducer.Tx<ReplicaMessage> {
                    val tx = inner.openTx()
                    return object : Log.AtomicProducer.Tx<ReplicaMessage> by tx {
                        override fun appendMessage(message: ReplicaMessage): CompletableDeferred<Log.MessageMetadata> {
                            val real = tx.appendMessage(message)
                            appendStarted.complete(Unit)
                            return CompletableDeferred<Log.MessageMetadata>().also { gated ->
                                backgroundScope.launch { gate.await(); gated.complete(real.await()) }
                            }
                        }
                    }
                }

                override fun close() = inner.close()
            }
        }

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapProducer = wrapProducer,
        )

        // Capture the executeTx failure; the runTest timeout guards against a hang if it never completes.
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.executeTx(null) { TxIndexer.TxResult.Committed() }
            } catch (e: CancellationException) {
                thrown.complete(e)
                throw e
            } catch (e: Throwable) {
                thrown.complete(e)
            }
        }

        appendStarted.await()

        // Cancel the leader term — the gate will never open, so without term-close propagation
        // executeTx would hang until the runTest timeout.
        lp.cancelAndJoin()

        // If executeTx hangs, thrown never completes and runTest's timeout fires — that's the hang guard.
        thrown.await()
    }

    @Test
    fun `closing the leader term fails a buffered, never-received executeTx`() = runTest(timeout = 5.seconds) {
        val writerEntered = CompletableDeferred<Unit>()
        val writerGate = CompletableDeferred<Unit>()

        val lp = leaderProc(StandardTestDispatcher(testScheduler))

        // t1 parks the persister inside its writer, so t2's task sits buffered in the channel —
        // never received, so never staged: only the exit drain can unblock its caller.
        val t1 = backgroundScope.async {
            lp.executeTx(null) { writerEntered.complete(Unit); writerGate.await(); TxIndexer.TxResult.Committed() }
        }
        writerEntered.await()
        val t2 = backgroundScope.async { lp.executeTx(null) { TxIndexer.TxResult.Committed() } }
        testScheduler.advanceUntilIdle()

        lp.cancelAndJoin()

        // t1 fails via the pre-stage catch (cancelled mid-writer); t2 via the buffered-task drain.
        // A hang on either fires runTest's timeout.
        assertTrue(runCatching { t1.await() }.isFailure, "the in-writer executeTx must fail, not hang")
        assertTrue(runCatching { t2.await() }.isFailure, "the buffered executeTx must fail, not hang")
    }

    @Test
    fun `a slow append pipelines subsequent ext-source txs`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()
        val producerBatchSizes = mutableListOf<Int>()

        // Counts each producer transaction's size, and gates every append handle so the first batch's
        // settle stalls on the gate (a slow commit-ack) without blocking any thread.
        val wrapProducer = { inner: Log.AtomicProducer<ReplicaMessage> ->
            object : Log.AtomicProducer<ReplicaMessage> {
                override fun openTx(): Log.AtomicProducer.Tx<ReplicaMessage> {
                    val tx = inner.openTx()
                    var count = 0
                    return object : Log.AtomicProducer.Tx<ReplicaMessage> by tx {
                        override fun appendMessage(message: ReplicaMessage): CompletableDeferred<Log.MessageMetadata> {
                            count++
                            val real = tx.appendMessage(message)
                            appendStarted.complete(Unit)
                            return CompletableDeferred<Log.MessageMetadata>().also { gated ->
                                backgroundScope.launch { gate.await(); gated.complete(real.await()) }
                            }
                        }

                        override fun commit() {
                            tx.commit()
                            producerBatchSizes += count
                        }
                    }
                }

                override fun close() = inner.close()
            }
        }

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            // Explicit null head: the relaxed default returns a mock TransactionKey whose txId is 0,
            // which would seed the staging index at 0 and shift the ext-source txIds to 1..5.
            liveIndex = mockk(relaxed = true) { every { latestCompletedTx } returns null },
            wrapProducer = wrapProducer,
        )

        // tx 0: stages an ext-source tx and kicks the gated append
        lp.submitTx(null) { TxIndexer.TxResult.Committed() }
        appendStarted.await()

        // txs 1-4: submitted while the append is in-flight; they pipeline behind it.
        // Launched so the test body doesn't block on the cap-1 channel send.
        repeat(4) { backgroundScope.launch { lp.submitTx(null) { TxIndexer.TxResult.Committed() } } }

        testScheduler.advanceUntilIdle()

        // Open the gate: the in-flight append for tx0 completes; the persister settles it via the
        // select arm and kicks whatever has accumulated behind it.
        gate.complete(Unit)

        // Durability of tx4 confirms the full pipeline drained — every settle runs through the
        // select arm, so a broken arm shows up here as a hang, not a wrong count.
        watchers.awaitTx(4)

        // The batch SHAPE is emergent: scheduling decides how many producer transactions carry
        // txs 1-4 (staging crosses launch → channel → persister, unlike a source-log poll batch),
        // so assert correctness-under-accumulation and leave the coalescing factor to the
        // kafka-source benchmark. tx 0 always seals alone — nothing else was staged when it kicked.
        assertEquals(1, producerBatchSizes.first(), "tx 0 kicked its append alone")
        assertEquals(5, producerBatchSizes.sum(), "every tx reached the replica log exactly once")

        val resolvedTxs = replicaLog.readRecords(0, replicaLog.latestSubmittedMsgId + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()
        assertEquals((0 until 5L).toList(), resolvedTxs.map { it.txId }, "all 5 txs land, in send order")
    }

    @Test
    fun `block boundaries carry the latest external-source token, not the last tx's`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)

        // Test-controlled block fullness: false while the ext-source tx settles, flipped to true so
        // the cut lands at the settle of a batch whose LAST tx is a token-less source-log tx.
        val cutNow = AtomicBoolean(false)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { finishBlock(any(), any()) } returns emptyMap()
            every { latestCompletedTx } returns null
            every { isFull() } answers { cutNow.get() }
        }
        val trieCatalog = mockk<TrieCatalog>(relaxed = true) {
            every { getPartitions(any()) } returns emptyList()
        }
        val tableCatalog = mockk<TableCatalog>(relaxed = true) {
            every { finishBlock(any(), any(), any()) } returns emptyMap()
        }
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val blockCatalog = BlockCatalog("test", null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1)

        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null, null, backgroundScope, StandardTestDispatcher(testScheduler))

        val lp = LeaderLogProcessor(
            allocator, nodeBase, dbStorage, mockk(relaxed = true),
            dbState, blockUploader, watchers,
            extSource = null, replicaProducer = replicaProducer,
            skipTxs = setOf(10), dbCatalog = null,
            partition = 0, afterReplicaMsgId = -1,
            scope = backgroundScope,
        ).also(leadersToClose::add)

        val token = byteArrayOf(1, 2, 3)

        // The ext-source tx carries the CDC resume token; awaiting its durability (txId 0) pins the
        // ordering without any producer gating — no block is cut while cutNow is false.
        lp.submitTx(token) { TxIndexer.TxResult.Committed() }
        watchers.awaitTx(0)

        cutNow.set(true)

        // Source-log record with msgId 10 (skipTxs covers it, so no Arrow payload needed; and its
        // txId must exceed the ext tx's for watchers' monotonicity check). Its settle cuts the block:
        // the batch's last tx is this token-less source-log tx, so txs.last() would write a null
        // token — the boundary must instead carry the ext tx's token from the watchers' view.
        lp.processRecords(listOf(
            Log.Record(0, 10, Instant.now(), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
        ))

        val boundaries = replicaLog.readRecords(0, replicaLog.latestSubmittedMsgId + 1)
            .mapNotNull { it.message as? ReplicaMessage.BlockBoundary }.toList()

        assertEquals(1, boundaries.size, "exactly one BlockBoundary should be written")
        assertArrayEquals(
            token, boundaries.single().externalSourceToken,
            "BlockBoundary must carry the ext-source tx's token, not the source-log tx's null token"
        )
    }
}
