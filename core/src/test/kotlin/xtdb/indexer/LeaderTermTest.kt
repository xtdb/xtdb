package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import xtdb.NodeBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.DatabaseName
import xtdb.api.IndexerConfig
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.PartitionLog
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.tx.ExternalSource
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import xtdb.types.MessageId
import xtdb.util.closeAll
import java.time.InstantSource

/**
 * Stands up a leader term without a transport, for tests of anything the term drives.
 *
 * Nearly everything the leader side does is only observable with a term running — a resolved tx reaches
 * the replica log through the append pump, and reaches the live index only on consume-back — so a test of
 * the source-log processor or the append pump needs the same fixture as a test of the leader itself. That
 * is what this is for, and why it is shared rather than owned by any one of those files.
 */
internal abstract class LeaderTermTest {

    protected lateinit var nodeBase: NodeBase
    protected lateinit var allocator: BufferAllocator

    // runTest cancels and joins backgroundScope before tearDown, so the leaders are quiescent here
    // and freed before `allocator` closes.
    private val leadersToClose = mutableListOf<AutoCloseable>()

    @BeforeEach
    fun setUpTerm() {
        nodeBase = NodeBase.openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDownTerm() {
        leadersToClose.closeAll()
        allocator.close()
        nodeBase.close()
    }

    /**
     * A relaxed [LiveIndex] mock carrying the production block threshold. Worth stubbing explicitly:
     * a bare relaxed mock answers 0 for `rowsPerBlock`, which the leader's resolve-side gauge reads as
     * "cut a block on every tx". These tests drive their cuts explicitly, via FlushBlock or the CAS.
     */
    protected fun liveIndexMock(configure: LiveIndex.() -> Unit = {}) =
        mockk<LiveIndex>(relaxed = true) {
            every { rowsPerBlock } returns IndexerConfig().rowsPerBlock
            configure()
        }

    // Records what was asked of it, so a test can ask what this node's set holds rather than what was
    // called. Nothing here opens a database, so a name is attached the moment it is asked for.
    protected class RecordingDbCatalog : Database.Catalog {
        val attached = mutableListOf<DatabaseName>()

        override val databaseNames get() = attached.toSet()
        override val txScoped = false
        override fun databaseOrNull(dbName: DatabaseName): Database? = null

        override fun checkCanAttach(dbName: DatabaseName, config: Database.Config) {}
        override fun checkCanDetach(dbName: DatabaseName) {}

        override fun attach(dbName: DatabaseName, config: Database.Config?) {
            attached += dbName
        }

        override fun detach(dbName: DatabaseName) {
            attached -= dbName
        }
    }

    // Decorate a driver so its replica-log append blocks on [gate] before the message lands, completing
    // [appendStarted] the first time the pump reaches it. Models a slow append-ack: while the gate is shut
    // the message is not on the log, so it can't be consumed back — the ReadIndex ack (and thus executeTx)
    // stays pending. Everything else, the tail included, delegates to the real driver.
    protected fun gatedDriver(
        inner: LeaderDriver, gate: CompletableDeferred<Unit>, appendStarted: CompletableDeferred<Unit>,
    ): LeaderDriver = object : LeaderDriver by inner {
        override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata {
            appendStarted.complete(Unit)
            gate.await()
            return inner.appendToReplica(msg)
        }
    }

    /**
     * Start a term the way [LogProcessor] does: the term and a replica-log reader feeding it launched into
     * the term's own job, so cancelling that job is what stops it, and freed once the test has joined it.
     *
     * The reader stands in for the partition's tail, which in production outlives the term — so it is the
     * term ending that has to stop it here.
     */
    protected fun CoroutineScope.startTerm(
        partitionStorage: PartitionStorage,
        replicaAppender: ReplicaLogAppender,
        watchers: Watchers,
        proc: LeaderLogProcessor,
    ) =
        proc.also {
            leadersToClose += it
            val replicaMsgs = Channel<ReplicaApply>()
            launch {
                val reader = launch {
                    partitionStorage.replicaLog.tailAll(-1) { records ->
                        records.forEach { replicaMsgs.applyAndAwait(it) }
                    }
                }

                try {
                    runLeaderTerm(
                        "test", watchers, proc, replicaMsgs, replicaAppender,
                        partitionStorage.sourceLog, resumeAfterMsgId = -1
                    )
                } finally {
                    reader.cancel()
                }
            }
        }

    protected fun TestScope.leaderProc(
        uploadDispatcher: CoroutineDispatcher,
        sourceLog: InMemoryLog<SourceMessage> = InMemoryLog(InstantSource.system(), 0),
        replicaLog: InMemoryLog<ReplicaMessage> = InMemoryLog(InstantSource.system(), 0),
        bufferPool: BufferPool = mockk(relaxed = true) { every { epoch } returns 0 },
        liveIndex: LiveIndex = liveIndexMock(),
        trieCatalog: TrieCatalog = createTrieCatalog(),
        compactor: Compactor.ForDatabase = mockk(relaxed = true),
        watchers: Watchers = Watchers(latestTxId = -1, latestSourceMsgId = -1, latestReplicaMsgId = -1),
        // These tests submit through the processor rather than driving an adapter, so the source only has
        // to exist for the processor to.
        extSource: ExternalSource = mockk(relaxed = true),
        skipTxs: Set<MessageId> = emptySet(),
        leaderTerm: Long = 1,
        // A leader may only hold one if it is the primary's, so supplying one names this database 'xtdb'.
        dbCatalog: Database.Catalog? = null,
        wrapDriver: (LeaderDriver) -> LeaderDriver = { it },
        termJob: Job = SupervisorJob(backgroundScope.coroutineContext.job),
    ): LeaderLogProcessor {
        val dbName = if (dbCatalog != null) "xtdb" else "test"
        val tableCatalog = TableCatalog(bufferPool)
        val partitionState = PartitionState(tableCatalog, trieCatalog, liveIndex)
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader =
            BlockUploader(
                partitionStorage, partitionState, "xtdb", compactor, null, null,
                backgroundScope, uploadDispatcher
            )
        val driver = wrapDriver(RealLeaderDriver(partitionStorage, partitionState, blockUploader))

        val termScope = backgroundScope + termJob
        val replicaAppender = ReplicaLogAppender(driver, leaderTerm, NoAssertElectionDriver)

        return termScope.startTerm(
            partitionStorage, replicaAppender, watchers,
            LeaderLogProcessor(
                allocator, nodeBase, partitionStorage, mockk(relaxed = true),
                partitionState, dbName,
                driver, watchers, replicaAppender, extSource,
                skipTxs = skipTxs, dbCatalog = dbCatalog,
                leaderTerm = leaderTerm,
                flushTimeout = IndexerConfig().flushDuration,
            )
        )
    }

    /**
     * A term wired up but not started, so a test can drive [runLeaderTerm] or one of its components
     * directly and have it return. [leaderProc] launches the term into a scope instead, where the only
     * handle on its completion is scheduler advancement.
     */
    protected fun TestScope.unstartedTerm(
        watchers: Watchers,
        driver: (LeaderDriver) -> LeaderDriver = { it },
        extSource: ExternalSource? = null,
    ): Triple<LeaderLogProcessor, ReplicaLogAppender, PartitionLog<SourceMessage>> {
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val partitionState =
            PartitionState(TableCatalog(bufferPool), createTrieCatalog(), liveIndexMock())
        val partitionStorage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)
        val blockUploader = BlockUploader(
            partitionStorage, partitionState, "xtdb", mockk(relaxed = true), null, null,
            backgroundScope, StandardTestDispatcher(testScheduler)
        )
        val leaderDriver = driver(RealLeaderDriver(partitionStorage, partitionState, blockUploader))
        val appender = ReplicaLogAppender(leaderDriver, leaderTerm = 1, NoAssertElectionDriver)

        val proc = LeaderLogProcessor(
            allocator, nodeBase, partitionStorage, mockk(relaxed = true),
            partitionState, "test", leaderDriver, watchers, appender,
            extSource,
            skipTxs = emptySet(), dbCatalog = null,
            leaderTerm = 1,
            flushTimeout = IndexerConfig().flushDuration,
        )
        leadersToClose += proc
        return Triple(proc, appender, partitionStorage.sourceLog)
    }
}
