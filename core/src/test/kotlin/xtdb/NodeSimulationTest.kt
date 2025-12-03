package xtdb

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import xtdb.SimulationTestUtils.Companion.L1TrieKeys
import xtdb.SimulationTestUtils.Companion.addTriesToBufferPool
import xtdb.SimulationTestUtils.Companion.buildTrieDetails
import xtdb.SimulationTestUtils.Companion.createJobCalculator
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.SimulationTestUtils.Companion.prefix
import xtdb.SimulationTestUtils.Companion.setLogLevel
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.arrow.unsupported
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.compactor.CompactorDriverConfig
import xtdb.compactor.CompactorMockDriver
import xtdb.compactor.RepeatableSimulationTest
import xtdb.database.DatabaseName
import xtdb.database.IDatabase
import xtdb.garbage_collector.GarbageCollector
import xtdb.garbage_collector.GarbageCollectorMockDriver
import xtdb.indexer.Indexer
import xtdb.indexer.LogProcessor
import xtdb.log.proto.TrieDetails
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.storage.MemoryStorage
import xtdb.table.TableRef
import xtdb.trie.TrieCatalog
import xtdb.util.asPath
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.microseconds

data class MockDatabase(
    override val name: DatabaseName,
    override val allocator: BufferAllocator,
    override val bufferPool: BufferPool,
    override val trieCatalog: TrieCatalog,
    override val blockCatalog: BlockCatalog,
    private val compactorOrNull: Compactor.ForDatabase?,
) : IDatabase {
    override val compactor: Compactor.ForDatabase get() = compactorOrNull ?: error("compactor not initialised")
    override val tableCatalog: TableCatalog get() = unsupported("tableCatalog")
    override val log: Log get() = unsupported("log")
    override val metadataManager: PageMetadata.Factory get() = unsupported("metadataManager")
    override val logProcessor: LogProcessor get() = unsupported("logProcessor")
    override val txSink: Indexer.TxSink get() = unsupported("txSink")
    fun withCompactor(compactor: Compactor.ForDatabase) = copy(compactorOrNull = compactor)
}

fun listTrieNamesFromBufferPool(bufferPool: BufferPool, tableRef: TableRef): List<String> =
    bufferPool
        .listAllObjects("tables/public\$${tableRef.tableName}/data/".asPath)
        .map { it.key.fileName.toString().removeSuffix(".arrow") }

@ParameterizedTest(name = "[iteration {0}]")
@MethodSource("xtdb.SimulationTestBase#iterationSource")
annotation class RepeatableSimulationTest

@Tag("property")
class NodeSimulationTest : SimulationTestBase() {
    val garbageLifetime = Duration.ofSeconds(60)
    private lateinit var allocator: BufferAllocator
    private lateinit var bufferPool: MemoryStorage
    private lateinit var compactorDriver: CompactorMockDriver
    private lateinit var compactor: Compactor
    private lateinit var compactorForDb: Compactor.ForDatabase
    private lateinit var blockCatalog: BlockCatalog
    private lateinit var trieCatalog: TrieCatalog
    private lateinit var db: MockDatabase
    private lateinit var garbageCollector: GarbageCollector


    @BeforeEach
    fun setUp() {
        super.setUpSimulation()
        setLogLevel.invoke("xtdb".symbol, "DEBUG")
        val jobCalculator = createJobCalculator.invoke() as Compactor.JobCalculator

        compactorDriver = CompactorMockDriver(dispatcher, currentSeed, CompactorDriverConfig())
        allocator = RootAllocator()
        bufferPool = MemoryStorage(allocator, epoch = 0)
        compactor = Compactor.Impl(compactorDriver, null, jobCalculator, false, 2, dispatcher)
        trieCatalog = createTrieCatalog.invoke(mutableMapOf<Any, Any>(), 100 * 1024 * 1024) as TrieCatalog
        blockCatalog = BlockCatalog("xtdb", bufferPool)
        val uninitializedDb = MockDatabase("xtdb", allocator, bufferPool, trieCatalog, blockCatalog, null)
        compactorForDb = compactor.openForDatabase(uninitializedDb)
        db = uninitializedDb.withCompactor(compactorForDb)
        garbageCollector = GarbageCollector.Impl(
            db = db,
            driverFactory = GarbageCollectorMockDriver(),
            blocksToKeep = 2,
            garbageLifetime = garbageLifetime,
            approxRunInterval = Duration.ofSeconds(30),
            coroutineCtx = dispatcher
        )
    }

    @AfterEach
    fun tearDown() {
        super.tearDownSimulation()
        garbageCollector.close()
        bufferPool.close()
        compactorForDb.close()
        compactor.close()
        allocator.close()
    }

    private fun addTries(tableRef: TableRef, tries: List<TrieDetails>, timestamp: Instant) {
        tries.forEach {
            compactorDriver.trieKeyToFileSize[it.trieKey.toString()] = it.dataFileSize
        }
        db.trieCatalog.addTries(tableRef, tries, timestamp)
        addTriesToBufferPool(bufferPool, tableRef, tries)
    }


    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `l1 compaction followed by garbage collection`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val l1Tries = L1TrieKeys.take(4).toList()

        addTries(
            table,
            l1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
Instant.now()
        )

        for (blockIndex in 1L..3L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        Assertions.assertEquals(l1Tries, trieCatalog.listAllTrieKeys(table), "l1s present in trie catalog")
        Assertions.assertEquals(l1Tries, listTrieNamesFromBufferPool(bufferPool, table), "l1s present in buffer pool")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        Assertions.assertEquals(l1Tries, trieCatalog.listAllTrieKeys(table), "live l1s haven't been garbage collected")
        Assertions.assertEquals(l1Tries, listTrieNamesFromBufferPool(bufferPool, table), "live l1s haven't been garbage collected")

        compactorForDb.compactAll()

        val l2Tries = listOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03")
        val allTries = (l1Tries + l2Tries).toSet()

        Assertions.assertEquals(allTries, listTrieNamesFromBufferPool(bufferPool, table).toSet(), "l2s present in buffer pool")
        Assertions.assertEquals(allTries, trieCatalog.listAllTrieKeys(table).toSet(), "l2s present in trie catalog")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        Assertions.assertEquals(l2Tries.toSet(), trieCatalog.listAllTrieKeys(table).toSet(), "l1s should get garbage collected from trie catalog")
        Assertions.assertEquals(l2Tries.toSet(), listTrieNamesFromBufferPool(bufferPool, table).toSet(), "l1s should get garbage collected from buffer pool")
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `gc during compaction preserves files`(iteration: Int)  {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val l1Tries = L1TrieKeys.take(8).toList()
        val testScope = CoroutineScope(dispatcher)
        val expectedL2Tries = listOf(
            "l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03",
            "l02-rc-p0-b07", "l02-rc-p1-b07", "l02-rc-p2-b07", "l02-rc-p3-b07"
        )

        addTries(
            table,
            l1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks so GC can consider tries for collection
        for (blockIndex in 5L..7L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        Assertions.assertEquals(l1Tries, trieCatalog.listAllTrieKeys(table), "l1s present initially")
        Assertions.assertEquals(l1Tries, listTrieNamesFromBufferPool(bufferPool, table), "l1s present in buffer pool initially")

        // Launch both compaction and GC on the dispatcher using coroutines
        runBlocking(dispatcher) {
            val compactionJob = launch {
                compactorForDb.startCompaction().await()
            }
            val gcJob = launch {
                val asOf = Instant.now() + Duration.ofHours(1)
                while(trieCatalog.garbageTries(table, asOf).isEmpty()) { yield() } // wait for compaction to mark tries as garbage
                garbageCollector.garbageCollectTries(asOf)
            }

            compactionJob.join()
            gcJob.join()
        }

        val triesInCatalog = trieCatalog.listAllTrieKeys(table)
        val triesInBufferPool = listTrieNamesFromBufferPool(bufferPool, table)

        Assertions.assertTrue(expectedL2Tries.all { it in triesInCatalog }, "All L2 tries should be present in catalog.")
        Assertions.assertTrue(expectedL2Tries.all { it in triesInBufferPool }, "All L2 tries should be present in buffer pool.")

        val l1sInCatalog = triesInCatalog.prefix("l01-rc-")
        Assertions.assertTrue(l1sInCatalog.size < 8, "Some l1s should get garbage collected from trie catalog mid compaction.")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        Assertions.assertEquals(expectedL2Tries.toSet(), trieCatalog.listAllTrieKeys(table).toSet(), "l1s should get garbage collected from trie catalog, leaving l2s")
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `gc collects old garbage while compaction runs`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L

        val oldl1Tries = L1TrieKeys.take(4).toList()
        val oldl2Tries = listOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03")
        val oldTries = oldl1Tries + oldl2Tries

        addTries(
            table,
            oldTries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        Assertions.assertEquals(oldTries, trieCatalog.listAllTrieKeys(table), "tries present in catalog")
        Assertions.assertEquals(oldTries, listTrieNamesFromBufferPool(bufferPool, table), "tries present in buffer pool")

        val newL1Tries = listOf("l01-rc-b04", "l01-rc-b05", "l01-rc-b06", "l01-rc-b07")
        addTries(
            table,
            newL1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks so GC can consider tries for collection
        for (blockIndex in 5L..7L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        val expectedNewL2Tries = listOf("l02-rc-p0-b07", "l02-rc-p1-b07", "l02-rc-p2-b07", "l02-rc-p3-b07")

        // Launch compaction (for new L1s) and GC (for old garbage L1s) in parallel
        runBlocking(dispatcher) {
            val compactionJob = launch {
                compactorForDb.startCompaction().await()
            }
            val gcJob = launch {
                garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }

            compactionJob.join()
            gcJob.join()
        }

        val finalTriesInCatalog = trieCatalog.listAllTrieKeys(table).toSet()
        val finalTriesInBufferPool = listTrieNamesFromBufferPool(bufferPool, table).toSet()

        Assertions.assertTrue(oldl1Tries.none { it in finalTriesInCatalog }, "Old L1 tries should be garbage collected")
        Assertions.assertTrue(oldl1Tries.none { it in finalTriesInBufferPool }, "Old L1 tries should be removed from buffer pool")

        Assertions.assertTrue(expectedNewL2Tries.all { it in finalTriesInCatalog }, "New L2 tries should be created by compaction")
        Assertions.assertTrue(expectedNewL2Tries.all { it in finalTriesInBufferPool }, "New L2 tries should be in buffer pool")

        Assertions.assertTrue(oldl2Tries.all { it in finalTriesInCatalog }, "Old L2 tries should still be present")
        Assertions.assertTrue(oldl2Tries.all { it in finalTriesInBufferPool }, "Old L2 tries should still be in buffer pool")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }
        val l2Set = (oldl2Tries + expectedNewL2Tries).toSet()
        Assertions.assertEquals(l2Set.toSet(), trieCatalog.listAllTrieKeys(table).toSet(), "l1s should get garbage collected from trie catalog, leaving l2s")
    }
}