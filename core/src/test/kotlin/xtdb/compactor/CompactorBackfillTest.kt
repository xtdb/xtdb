package xtdb.compactor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.SimulationTestUtils.Companion.createPartitionValue
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.IndexOp
import xtdb.api.log.SourceMessage.IndexBackfilled
import xtdb.api.log.SourceMessage.TriesAdded
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.block.proto.block
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.index.IndexDef
import xtdb.index.IndexState
import xtdb.index.IndexTrie
import xtdb.storage.MemoryStorage
import xtdb.table.BlockMetadata
import xtdb.table.fromSchemaAndTable
import xtdb.trie.BlockIndex
import xtdb.trie.Trie
import xtdb.trie.TrieCatalog
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class CompactorBackfillTest {

    private val def = IndexDef(UUID.randomUUID(), "by_email", TableRef("public", "users"), "email")

    private val dataJob = object : Compactor.Job {
        override val table = fromSchemaAndTable("foo")
        override val trieKeys = listOf(Trie.l0Key(0).toString())
        override val part = ByteArray(0)
        override val outputTrieKey = Trie.Key(1, null, null, 0)
        override val partitionedByRecency = false
    }

    private val running = AtomicInteger()
    private val maxRunning = AtomicInteger()
    private val dataJobRan = AtomicBoolean()
    private val backfilledBlocks = mutableListOf<BlockIndex>()

    private suspend fun <T> tracked(f: suspend () -> T): T {
        maxRunning.accumulateAndGet(running.incrementAndGet(), ::maxOf)
        try {
            delay(50)
            return f()
        } finally {
            running.decrementAndGet()
        }
    }

    private lateinit var allocator: BufferAllocator
    private lateinit var storage: MemoryStorage
    private lateinit var scope: CoroutineScope
    private lateinit var partitionState: PartitionState
    private lateinit var compactor: Compactor.ForDatabase

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        storage = MemoryStorage(allocator, epoch = 0)
        scope = CoroutineScope(Dispatchers.Default)
        val block = block { blockIndex = 2 }
        partitionState = PartitionState.open(
            allocator,
            createPartitionValue(block).let { pv ->
                pv.withTableCat(pv.tableCat.refreshed(block, mapOf(def.table to BlockMetadata(emptyMap(), 1, emptyMap()))))
            },
        )

        val driverFactory = object : Compactor.Driver.Factory {
            override fun create(
                allocator: BufferAllocator, partitionStorage: PartitionStorage,
                partitionState: PartitionState, watchers: Watchers
            ) = object : Compactor.Driver {
                override suspend fun executeJob(job: Compactor.Job) =
                    tracked { dataJobRan.set(true); TriesAdded(Storage.VERSION, 0, emptyList()) }

                override suspend fun publishTries(triesAdded: TriesAdded) = Unit

                override suspend fun executeBackfill(index: IndexDef, block: BlockIndex) =
                    tracked { IndexBackfilled(Storage.VERSION, 0, index.id, block, if (block == 1L) 1024 else null) }

                override suspend fun publishBackfill(backfilled: IndexBackfilled) {
                    synchronized(backfilledBlocks) { backfilledBlocks += backfilled.blockIndex }
                    partitionState.indexBackfilled(backfilled.indexId, backfilled.blockIndex, backfilled.l0)
                }

                override fun close() = Unit
            }
        }

        val jobCalculator = object : Compactor.JobCalculator {
            override fun availableJobs(trieCatState: TrieCatalog.State) =
                if (dataJobRan.get()) emptyList() else listOf(dataJob)
        }

        compactor = Compactor.Impl(driverFactory, null, jobCalculator, false, 1)
            .openForDatabase(
                scope, allocator,
                PartitionStorage(DatabaseLogs(null, null), storage, null),
                partitionState,
                Watchers(latestTxId = -1, latestSourceMsgId = -1)
            )
    }

    @AfterEach
    fun tearDown() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        compactor.close()
        partitionState.close()
        storage.close()
        allocator.close()
    }

    private fun declare() =
        partitionState.commitTx(TransactionKey(0, Instant.EPOCH), emptyMap(), IndexOp.Create(def))

    @Test
    fun `every owed block is backfilled once, sharing the configured job concurrency with data jobs`() = runBlocking {
        declare()
        withTimeout(30_000) { compactor.compactAll() }

        assertEquals(listOf(0L, 1L, 2L), synchronized(backfilledBlocks) { backfilledBlocks.sorted() })
        assertEquals(IndexState.Building(emptySet()), partitionState.indexCatalog[def.id]?.state)
        assertEquals(listOf(IndexTrie(Trie.l0Key(1), 1024)), partitionState.indexCatalog[def.id]?.tries)

        assertEquals(true, dataJobRan.get())
        assertEquals(1, maxRunning.get(), "one job at a time, with one configured, across data and backfill jobs")
    }

    @Test
    fun `a declaration owing blocks wakes the compactor, with no block to signal it`() = runBlocking {
        withTimeout(30_000) { compactor.compactAll() }
        assertEquals(true, dataJobRan.get())

        declare()

        withTimeout(30_000) {
            partitionState.indexCatalogFlow.first { (it[def.id]?.state as? IndexState.Building)?.complete == true }
        }
        assertEquals(listOf(0L, 1L, 2L), synchronized(backfilledBlocks) { backfilledBlocks.sorted() })
    }
}
