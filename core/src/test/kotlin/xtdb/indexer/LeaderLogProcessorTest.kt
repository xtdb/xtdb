package xtdb.indexer

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.NodeBase
import xtdb.api.IndexerConfig
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
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.storage.BufferPool
import xtdb.table.fromSchemaAndTable
import xtdb.trie.TrieCatalog
import xtdb.util.closeAll
import java.time.Instant
import java.time.InstantSource
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds
import xtdb.api.tx.ExternalSource
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

    /**
     * A relaxed [LiveIndex] mock carrying the production block threshold. Worth stubbing explicitly:
     * a bare relaxed mock answers 0 for `rowsPerBlock`, which the leader's resolve-side gauge reads as
     * "cut a block on every tx". These tests drive their cuts explicitly, via FlushBlock or the CAS.
     */
    private fun liveIndexMock(configure: LiveIndex.() -> Unit = {}) =
        mockk<LiveIndex>(relaxed = true) {
            every { rowsPerBlock } returns IndexerConfig().rowsPerBlock
            configure()
        }

    /**
     * Start a term the way [LogProcessor] does: the term and the partition's replica-log reader launched
     * into the term's own job, so cancelling that job is what stops it, and freed once the test has joined it.
     */
    private fun CoroutineScope.startTerm(
        partitionStorage: PartitionStorage,
        replicaAppender: ReplicaLogAppender,
        watchers: Watchers,
        proc: LeaderLogProcessor,
    ) =
        proc.also {
            leadersToClose += it
            launch {
                launch {
                    partitionStorage.replicaLog.tailAll(-1) { records ->
                        records.forEach { proc.queueReplicaMessage(it) }
                    }
                }
                launch { proc.gc.runGc() }
                proc.extSrcProc?.let { extSrcProc -> launch { extSrcProc.run() } }
                runLeaderTerm("test", watchers, proc, replicaAppender)
            }
        }

    private fun TestScope.leaderProc(
        uploadDispatcher: CoroutineDispatcher,
        sourceLog: InMemoryLog<SourceMessage> = InMemoryLog(InstantSource.system(), 0),
        replicaLog: InMemoryLog<ReplicaMessage> = InMemoryLog(InstantSource.system(), 0),
        bufferPool: BufferPool = mockk(relaxed = true) { every { epoch } returns 0 },
        liveIndex: LiveIndex = liveIndexMock(),
        blockCatalog: BlockCatalog = BlockCatalog(null),
        trieCatalog: TrieCatalog = mockk(relaxed = true),
        compactor: Compactor.ForDatabase = mockk(relaxed = true),
        watchers: Watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1),
        // These tests submit through the processor rather than driving an adapter, so the source only has
        // to exist for the processor to.
        extSource: ExternalSource = mockk(relaxed = true),
        skipTxs: Set<MessageId> = emptySet(),
        leaderTerm: Long = 1,
        wrapDriver: (LeaderDriver) -> LeaderDriver = { it },
        termJob: Job = SupervisorJob(backgroundScope.coroutineContext.job),
    ): LeaderLogProcessor {
        val tableCatalog = mockk<TableCatalog>(relaxed = true)
        val partitionState = PartitionState(blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", compactor, null, null, backgroundScope, uploadDispatcher)
        val driver = wrapDriver(
            RealLeaderDriver(
                partitionStorage, partitionState, blockUploader
            )
        )

        val termScope = backgroundScope + termJob
        val replicaAppender = ReplicaLogAppender(driver)

        return termScope.startTerm(
            partitionStorage, replicaAppender, watchers,
            LeaderLogProcessor(
                allocator, nodeBase, partitionStorage, mockk(relaxed = true),
                partitionState, "test", driver, watchers, replicaAppender, extSource,
                skipTxs = skipTxs, dbCatalog = null,
                leaderTerm = leaderTerm,
                flushTimeout = IndexerConfig().flushDuration,
            )
        )
    }

    // Decorate a driver so its replica-log append blocks on [gate] before the message lands, completing
    // [appendStarted] the first time the pump reaches it. Models a slow append-ack: while the gate is shut
    // the message is not on the log, so it can't be consumed back — the ReadIndex ack (and thus executeTx)
    // stays pending. Everything else, the tail included, delegates to the real driver.
    private fun gatedDriver(
        inner: LeaderDriver, gate: CompletableDeferred<Unit>, appendStarted: CompletableDeferred<Unit>,
    ): LeaderDriver = object : LeaderDriver by inner {
        override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata {
            appendStarted.complete(Unit)
            gate.await()
            return inner.appendToReplica(msg)
        }
    }

    @Test
    fun `TriesAdded forwarded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val trieCatalog = mockk<TrieCatalog>(relaxed = true)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)
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
        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.TriesAdded(Storage.VERSION, 0, tries))
        ))
        watchers.awaitSource(0)

        verify { trieCatalog.addTries(any(), any(), any()) }
        assertTrue(replicaLog.latestSubmittedOffset() >= 0, "replica log should have received a message")
    }

    @Test
    fun `FlushBlock triggers block finish when CAS matches`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val liveIndex = liveIndexMock {
            coEvery { finishBlock(any(), any()) } returns emptyMap()
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
        val blockCatalog = BlockCatalog(null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val partitionState = PartitionState(blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", compactor, null, null, backgroundScope, StandardTestDispatcher(testScheduler))
        val driver = RealLeaderDriver(
            partitionStorage, partitionState, blockUploader
        )
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val termScope = backgroundScope + SupervisorJob(backgroundScope.coroutineContext.job)
        val replicaAppender = ReplicaLogAppender(driver)
        val lp = termScope.startTerm(
            partitionStorage, replicaAppender, watchers,
            LeaderLogProcessor(
                allocator, nodeBase, partitionStorage, mockk(relaxed = true),
                partitionState, "test", driver, watchers, replicaAppender,
                extSource = null,
                skipTxs = emptySet(), dbCatalog = null,
                flushTimeout = IndexerConfig().flushDuration,
            )
        )

        val now = Instant.now()
        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
        ))
        watchers.awaitSource(0)

        coVerify { liveIndex.finishBlock(any(), eq(0)) }
        verify { liveIndex.nextBlock() }
        verify { compactor.signalBlock() }
        assertTrue(replicaLog.latestSubmittedOffset() >= 0, "replica log should have block messages")
    }

    @Test
    fun `FlushBlock ignored when CAS does not match`() = runTest {
        val liveIndex = liveIndexMock()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), liveIndex = liveIndex, watchers = watchers)

        val now = Instant.now()
        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(5))
        ))
        watchers.awaitSource(0)

        coVerify(exactly = 0) { liveIndex.finishBlock(any(), any()) }
    }

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

        val liveIndex = liveIndexMock {
            coEvery { finishBlock(any(), any()) } returns mapOf(tableRef to finishedBlock)
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
        val blockCatalog = BlockCatalog(null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val partitionState = PartitionState(blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", compactor, null, null, backgroundScope, StandardTestDispatcher(testScheduler))
        val driver = RealLeaderDriver(
            partitionStorage, partitionState, blockUploader
        )
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val termScope = backgroundScope + SupervisorJob(backgroundScope.coroutineContext.job)
        val replicaAppender = ReplicaLogAppender(driver)
        val lp = termScope.startTerm(
            partitionStorage, replicaAppender, watchers,
            LeaderLogProcessor(
                allocator, nodeBase, partitionStorage, mockk(relaxed = true),
                partitionState, "test", driver, watchers, replicaAppender,
                extSource = null,
                skipTxs = emptySet(), dbCatalog = null,
                flushTimeout = IndexerConfig().flushDuration,
            )
        )

        val now = Instant.now()
        lp.srcLogProc.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
        ))
        watchers.awaitSource(0)

        val replicaMessages = mutableListOf<ReplicaMessage>()
        backgroundScope.launch { replicaLog.tailAll(0, -1) { records ->
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
    fun `a slow append does not stall resolution`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        // Skipped txs each stage a real (aborted) row without needing a valid tx-ops payload.
        val n = 5L
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            skipTxs = (0 until n).toSet(),
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        val now = Instant.now()
        val records = (0 until n).map {
            Log.Record(0, it, now.plusMillis(it), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
        }

        // Resolution is decoupled from the append pump: the whole batch resolves and processRecords returns
        // even though the append is still stalled on the gate — reaching the assertions below is the proof.
        lp.srcLogProc.processRecords(records)
        appendStarted.await()
        assertFalse(gate.isCompleted, "sanity: nothing opened the append gate")

        // Once the append drains, every tx reaches the replica log — in send order.
        gate.complete(Unit)
        watchers.awaitTx(n - 1)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()
        assertEquals((0 until n).toList(), resolvedTxs.map { it.txId }, "all $n txs land, in send order")
    }

    @Test
    fun `executeTx returns only once its tx is durable`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // launch the executeTx so we can observe its completion state without blocking the test
        val txJob = backgroundScope.async { lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() } }

        appendStarted.await()

        assertFalse(txJob.isCompleted, "executeTx must not return before the replica-log append settles")

        gate.complete(Unit)
        val result = txJob.await()

        assertTrue(result is TransactionResult.Committed, "executeTx returns Committed once durable")
    }

    @Test
    fun `closing the leader term fails an awaiting executeTx rather than hanging`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        // Gate that is never opened — the append will stall indefinitely unless the term is cancelled.
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
            termJob = termJob,
        )

        // Capture the executeTx failure; the runTest timeout guards against a hang if it never completes.
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() }
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
        termJob.cancelAndJoin()

        // If executeTx hangs, thrown never completes and runTest's timeout fires — that's the hang guard.
        thrown.await()
    }

    @Test
    fun `closing the leader term fails a buffered, never-received executeTx`() = runTest(timeout = 5.seconds) {
        val writerEntered = CompletableDeferred<Unit>()
        val writerGate = CompletableDeferred<Unit>()

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), termJob = termJob)

        // t1 parks the persister inside its writer, so t2's task sits buffered in the channel —
        // never received, so never staged: only the exit drain can unblock its caller.
        val t1 = backgroundScope.async {
            lp.extSrcProc!!.executeTx(null) { writerEntered.complete(Unit); writerGate.await(); TxIndexer.TxResult.Committed() }
        }
        writerEntered.await()
        val t2 = backgroundScope.async { lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() } }
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        // t1 fails via the pre-stage catch (cancelled mid-writer); t2 via the buffered-task drain.
        // A hang on either fires runTest's timeout.
        assertTrue(runCatching { t1.await() }.isFailure, "the in-writer executeTx must fail, not hang")
        assertTrue(runCatching { t2.await() }.isFailure, "the buffered executeTx must fail, not hang")
    }

    @Test
    fun `closing the leader term fails a buffered, never-received source-log batch`() = runTest(timeout = 5.seconds) {
        val writerEntered = CompletableDeferred<Unit>()
        val writerGate = CompletableDeferred<Unit>()
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val termJob = SupervisorJob(backgroundScope.coroutineContext.job)
        val lp = leaderProc(StandardTestDispatcher(testScheduler), watchers = watchers, termJob = termJob)

        // Park the persister inside an ext-source writer, so the source batch below lands in sourceLogCh's
        // buffer and is never received.
        backgroundScope.launch {
            runCatching {
                lp.extSrcProc!!.executeTx(null) { writerEntered.complete(Unit); writerGate.await(); TxIndexer.TxResult.Committed() }
            }
        }
        writerEntered.await()

        // processRecords stands in for the transport's poll thread: it awaits the batch's completion. If the
        // term dies without failing the buffered batch, this await never returns — the poll thread wedges,
        // the transport's unregister is never serviced, and DatabaseCatalog.close blows its bound (#5711).
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.srcLogProc.processRecords(listOf(
                    Log.Record(0, 0, Instant.now(), SourceMessage.Tx(ByteArray(0), null, ZoneId.of("UTC"), null, null))
                ))
                thrown.complete(AssertionError("processRecords returned normally"))
            } catch (e: CancellationException) {
                thrown.complete(e); throw e
            } catch (e: Throwable) {
                thrown.complete(e)
            }
        }
        testScheduler.advanceUntilIdle()

        termJob.cancelAndJoin()

        // A hang here fires runTest's timeout — that's the regression guard.
        val e = thrown.await()
        assertFalse(
            e is AssertionError,
            "processRecords must fail when the term closes on a buffered batch, not return normally"
        )

        // ...and it must fail as CANCELLATION. The transport treats anything else as a poll-loop failure and
        // unwinds openGroupSubscription into the Database scope's handler, which poisons the watchers — so a
        // benign teardown would present as a terminal query failure. See SourceBatch.abandon.
        assertTrue(e is CancellationException, "the poll thread must see cancellation, got: $e")
        assertNull(watchers.exception, "a benign term close must not poison the watchers")
    }

    @Test
    fun `a slow append pipelines subsequent ext-source txs`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            // Explicit null head: the relaxed default returns a mock TransactionKey whose txId is 0,
            // which would seed the queue at 0 and shift the ext-source txIds to 1..5.
            liveIndex = liveIndexMock { every { latestCompletedTx } returns null },
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // tx 0: stages an ext-source tx and kicks the gated append
        lp.extSrcProc!!.submitTx(null) { TxIndexer.TxResult.Committed() }
        appendStarted.await()

        // txs 1-4: submitted while the append is in-flight; they pipeline behind it — resolution and the
        // append pump are decoupled, so a stalled append doesn't block subsequent submitTx from resolving
        // and staging. Launched so the test body doesn't block on the cap-1 channel send.
        repeat(4) { backgroundScope.launch { lp.extSrcProc.submitTx(null) { TxIndexer.TxResult.Committed() } } }

        testScheduler.advanceUntilIdle()

        // Open the gate: the stalled appends drain, get consumed back, and the whole pipeline settles.
        gate.complete(Unit)

        // Durability of tx4 confirms the full pipeline drained in order — a stall that had blocked
        // resolution would show up here as a hang.
        watchers.awaitTx(4)

        val resolvedTxs = replicaLog.readRecords(0, 0, replicaLog.latestSubmittedMsgId() + 1)
            .mapNotNull { it.message as? ReplicaMessage.ResolvedTx }.toList()
        assertEquals((0 until 5L).toList(), resolvedTxs.map { it.txId }, "all 5 txs land, in send order")
    }

    @Test
    fun `a higher-term record read back resigns the leader`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        // Gate our own append so this leader's tx never lands: the only record on the log will be the
        // higher-term one injected below, so consume-back reaches it. Term fencing on read-back is the sole
        // split-brain guard now the transactional producer is gone (#5817) — this exercises the resign path.
        val gate = CompletableDeferred<Unit>() // never opened
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            leaderTerm = 1,
            wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // An executeTx staged and awaiting durability — the resignation must fail it, not hang it. Capture
        // its failure via a launch + Deferred: a failing `async` would propagate to the (non-supervisor)
        // backgroundScope and fail the test, so we don't await it directly.
        val thrown = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            try {
                lp.extSrcProc!!.executeTx(null) { TxIndexer.TxResult.Committed() }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                thrown.complete(e)
            }
        }
        appendStarted.await()

        // A newer leader (term 2) has written to our replica log. Injected straight onto the underlying log
        // (past the gate), so consume-back reads it while our own term-1 append is still stalled.
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2))

        // term 2 > our term 1 → we resign; the term tears down and fails everything staged.
        val e = thrown.await()
        assertTrue(
            generateSequence(e) { it.cause }.any { it.message?.contains("superseded") == true },
            "the awaiting executeTx surfaces the supersession, got: $e"
        )

        // A clean resignation is expected, not a query fault: the watchers must not be poisoned — the
        // transport re-follows on the next rebalance.
        assertNull(watchers.exception, "a clean resignation must not poison the watchers")
    }

    @Test
    fun `resigning cancels in-flight source batches rather than surfacing the supersession`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        // Never opened. The BlockBoundary's append hangs here, so the cut never reads back and resolution
        // stays paused — which is what makes this deterministic: batch #1 parks as `pausedBatch` and batch #2
        // stays buffered in the driver's source-batch pipe, so the term resigns with both in flight.
        val gate = CompletableDeferred<Unit>()
        val appendStarted = CompletableDeferred<Unit>()

        val lp = leaderProc(
            StandardTestDispatcher(testScheduler), replicaLog = replicaLog, watchers = watchers,
            leaderTerm = 1, wrapDriver = { gatedDriver(it, gate, appendStarted) },
        )

        // Two batches, each standing in for the transport's poll thread awaiting `processRecords`.
        fun pollThread(msgId: Long) = CompletableDeferred<Throwable>().also { outcome ->
            backgroundScope.launch {
                try {
                    lp.srcLogProc.processRecords(listOf(Log.Record(0, msgId, Instant.now(), SourceMessage.FlushBlock(-1))))
                    outcome.complete(AssertionError("processRecords returned normally"))
                } catch (e: Throwable) {
                    outcome.complete(e)
                }
            }
        }

        val paused = pollThread(0)          // cuts the block, then parks mid-batch
        appendStarted.await()               // the boundary hit the gated append ⇒ we are paused
        val buffered = pollThread(1)        // sent while paused ⇒ buffered, received by nobody
        testScheduler.advanceUntilIdle()

        // A newer leader writes at term 2 — injected past the gate, so consume-back reads it while paused
        // (replicaMsgs is the one select arm the pause leaves open) and the leader resigns.
        replicaLog.appendMessage(ReplicaMessage.NoOp(termId = 2))

        // Both must fail as CANCELLATION, not with the LeaderSupersededException. The poll thread awaits
        // these, and a non-cancellation escaping processRecords unwinds openGroupSubscription into the
        // Database scope's CoroutineExceptionHandler → notifyError, so a clean resignation would present to
        // queries as a terminal failure. See SourceBatch.abandon.
        for ((name, handle) in listOf("paused" to paused, "buffered" to buffered))
            assertTrue(
                handle.await() is CancellationException,
                "the $name batch must fail as cancellation, got: ${handle.await()}"
            )

        assertNull(watchers.exception, "a resignation must not poison the watchers")
    }

    @Test
    fun `block boundaries carry the latest external-source token, not the last tx's`() = runTest(timeout = 5.seconds) {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)

        val liveIndex = liveIndexMock {
            coEvery { finishBlock(any(), any()) } returns emptyMap()
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
        val blockCatalog = BlockCatalog(null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1)

        val partitionState = PartitionState(blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(partitionStorage, partitionState, "xtdb", compactor, null, null, backgroundScope, StandardTestDispatcher(testScheduler))
        val driver = RealLeaderDriver(
            partitionStorage, partitionState, blockUploader
        )

        val termScope = backgroundScope + SupervisorJob(backgroundScope.coroutineContext.job)
        val replicaAppender = ReplicaLogAppender(driver)
        val lp = termScope.startTerm(
            partitionStorage, replicaAppender, watchers,
            LeaderLogProcessor(
                allocator, nodeBase, partitionStorage, mockk(relaxed = true),
                partitionState, "test", driver, watchers, replicaAppender,
                extSource = mockk(relaxed = true),
                skipTxs = setOf(10), dbCatalog = null,
                flushTimeout = IndexerConfig().flushDuration,
            )
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
